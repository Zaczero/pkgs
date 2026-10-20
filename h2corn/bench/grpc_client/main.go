// The runtime owns gRPC. This program owns finite load and response oracles.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const service = "h2corn.interop.v1.NotebookService"

type plan struct {
	Endpoint       string  `json:"endpoint"`
	Descriptor     string  `json:"descriptor"`
	CA             string  `json:"ca"`
	Case           string  `json:"case"`
	Shape          string  `json:"shape"`
	Size           int     `json:"size"`
	Messages       int     `json:"messages"`
	Concurrency    int     `json:"concurrency"`
	Duration       float64 `json:"duration_seconds"`
	Requests       uint64  `json:"requests"`
	Drain          float64 `json:"drain_seconds"`
	Mode           string  `json:"mode"`
	VerifyIdentity bool    `json:"verify_identity"`
}

// Buckets have a relative width of 1%; reported percentiles are upper bounds.
type histogram map[int]uint64

func (h histogram) add(d time.Duration) {
	n := max(1, d.Nanoseconds())
	h[int(math.Floor(math.Log(float64(n))/math.Log(1.01)))]++
}

func (h histogram) percentiles() map[string]float64 {
	result := map[string]float64{}
	var total uint64
	for _, count := range h {
		total += count
	}
	if total == 0 {
		return result
	}
	for name, q := range map[string]float64{"p50": .5, "p90": .9, "p95": .95, "p99": .99} {
		target := uint64(math.Ceil(q * float64(total)))
		var seen uint64
		for i := 0; i < 4608; i++ {
			seen += h[i]
			if seen >= target {
				result[name] = math.Exp(float64(i+1)*math.Log(1.01)) / 1e9
				break
			}
		}
	}
	return result
}

type result struct {
	Attempted       uint64             `json:"attempted"`
	Started         uint64             `json:"started"`
	VerifiedOK      uint64             `json:"verified_ok"`
	Cancelled       uint64             `json:"expected_cancelled"`
	Deadlines       uint64             `json:"deadline_exceeded"`
	OtherNonOK      uint64             `json:"other_non_ok"`
	Invalid         uint64             `json:"invalid_response"`
	Unfinished      uint64             `json:"unfinished"`
	AdmissionFailed uint64             `json:"admission_failures"`
	Messages        uint64             `json:"verified_messages"`
	PayloadBytes    uint64             `json:"verified_payload_bytes"`
	RequestBytes    uint64             `json:"serialized_request_bytes"`
	ResponseBytes   uint64             `json:"serialized_response_bytes"`
	Statuses        map[string]uint64  `json:"statuses"`
	Errors          []string           `json:"errors"`
	Elapsed         float64            `json:"elapsed_seconds"`
	Window          float64            `json:"window_seconds"`
	Drain           float64            `json:"drain_seconds"`
	RPCPercentiles  map[string]float64 `json:"latency_percentiles"`
	MessageLatency  map[string]float64 `json:"message_latency_percentiles"`
	FirstLatency    map[string]float64 `json:"first_message_percentiles"`
	RPCHistogram    histogram          `json:"rpc_latency_histogram"`
	MessageHist     histogram          `json:"message_latency_histogram"`
	FirstHist       histogram          `json:"first_message_histogram"`
	CPUUser         float64            `json:"cpu_user_seconds"`
	CPUSystem       float64            `json:"cpu_system_seconds"`
	MaxRSSKiB       int64              `json:"max_rss_kib"`
	GoVersion       string             `json:"go_version"`
	GOMAXPROCS      int                `json:"gomaxprocs"`
	Connections     int                `json:"connections"`
	ResourceScope   string             `json:"resource_scope"`
	TLS             map[string]any     `json:"tls,omitempty"`
	Identity        string             `json:"server_identity,omitempty"`
}

type invalidResponse struct{ message string }

func (e invalidResponse) Error() string { return e.message }

func check(ok bool, message string) error {
	if !ok {
		return invalidResponse{message}
	}
	return nil
}

type callResult struct {
	messages, payloadBytes, requestBytes, responseBytes uint64
	messageLatency                                      []time.Duration
	first                                               time.Duration
}

type client struct {
	conn    *grpc.ClientConn
	methods protoreflect.ServiceDescriptor
	plan    plan
	tls     map[string]any
}

type connectionStats struct{ opened atomic.Uint64 }

func (*connectionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (*connectionStats) HandleRPC(context.Context, stats.RPCStats)                       {}
func (*connectionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *connectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnBegin); ok {
		s.opened.Add(1)
	}
}

func message(desc protoreflect.MessageDescriptor, fields map[string]any) *dynamicpb.Message {
	m := dynamicpb.NewMessage(desc)
	for name, value := range fields {
		field := desc.Fields().ByName(protoreflect.Name(name))
		switch v := value.(type) {
		case string:
			m.Set(field, protoreflect.ValueOfString(v))
		case []byte:
			m.Set(field, protoreflect.ValueOfBytes(v))
		case uint32:
			m.Set(field, protoreflect.ValueOfUint32(v))
		}
	}
	return m
}

func field(m proto.Message, name string) protoreflect.Value {
	r := m.ProtoReflect()
	return r.Get(r.Descriptor().Fields().ByName(protoreflect.Name(name)))
}

func (c *client) unaryControl(ctx context.Context, name string) (string, error) {
	method := c.methods.Methods().ByName("GetNote")
	in := message(method.Input(), map[string]any{"note_id": name})
	out := dynamicpb.NewMessage(method.Output())
	var remote peer.Peer
	err := c.conn.Invoke(ctx, "/"+service+"/GetNote", in, out, grpc.Peer(&remote))
	if info, ok := remote.AuthInfo.(credentials.TLSInfo); ok {
		c.tls = map[string]any{
			"version": tls.VersionName(info.State.Version),
			"cipher":  tls.CipherSuiteName(info.State.CipherSuite),
			"alpn":    info.State.NegotiatedProtocol,
		}
	}
	return field(out, "body").String(), err
}

func validateMetadata(headers, trailers metadata.MD, name string) error {
	if err := check(len(headers.Get("x-bench-case")) == 1 && headers.Get("x-bench-case")[0] == name,
		"initial benchmark metadata missing or incorrect"); err != nil {
		return err
	}
	return check(len(trailers.Get("x-bench-finished")) == 1 && trailers.Get("x-bench-finished")[0] == "yes",
		"terminal benchmark metadata missing or incorrect")
}

func (c *client) cancelProbe(ctx context.Context) (string, error) {
	method := c.methods.Methods().ByName("ListRevisions")
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.conn.NewStream(callCtx, &grpc.StreamDesc{ServerStreams: true}, "/"+service+"/ListRevisions")
	if err != nil {
		return "", err
	}
	if err := stream.SendMsg(message(method.Input(), map[string]any{"note_id": "cancel"})); err != nil {
		return "", err
	}
	if err := stream.CloseSend(); err != nil {
		return "", err
	}
	out := dynamicpb.NewMessage(method.Output())
	if err := stream.RecvMsg(out); err != nil {
		return "", err
	}
	if field(out, "revision").Uint() != 1 || field(out, "summary").String() != strings.Repeat("x", c.plan.Size) {
		return "", invalidResponse{"cancel probe received an invalid first message"}
	}
	cancel()
	if code := status.Code(stream.RecvMsg(dynamicpb.NewMessage(method.Output()))); code != codes.Canceled {
		return "", invalidResponse{fmt.Sprintf("cancel probe returned %s instead of Canceled", code)}
	}
	for {
		receipt, err := c.unaryControl(ctx, "__bench_state")
		if err != nil {
			return "", err
		}
		var snapshot struct {
			Active   int `json:"active"`
			Readers  int `json:"readers"`
			Finished int `json:"finished"`
		}
		if err := json.Unmarshal([]byte(receipt), &snapshot); err != nil {
			return "", err
		}
		if snapshot.Active == 0 && snapshot.Readers == 0 && snapshot.Finished == 1 {
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			return "", invalidResponse{"server handler did not settle after cancellation"}
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *client) call(ctx context.Context, number uint64) (callResult, error) {
	ctx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	p := c.plan
	start := time.Now()
	outcome := callResult{}
	id := fmt.Sprintf("%016x", number)
	payload := strings.Repeat("x", p.Size)
	name := map[string]string{"unary": "GetNote", "upload": "UploadNote", "download": "ListRevisions", "duplex": "Collaborate"}[p.Shape]
	method := c.methods.Methods().ByName(protoreflect.Name(name))
	requestID := id
	if p.Shape == "unary" {
		if p.Size == 0 {
			requestID = ""
		} else {
			requestID = id + strings.Repeat("x", p.Size-len(id))
		}
		in := message(method.Input(), map[string]any{"note_id": requestID})
		out := dynamicpb.NewMessage(method.Output())
		var headers, trailers metadata.MD
		err := c.conn.Invoke(ctx, "/"+service+"/"+name, in, out, grpc.Header(&headers), grpc.Trailer(&trailers))
		if err != nil {
			return outcome, err
		}
		if err := check(proto.Equal(out, message(method.Output(), map[string]any{"note_id": requestID})),
			"unary message differs from the complete expected response"); err != nil {
			return outcome, err
		}
		if err := validateMetadata(headers, trailers, p.Case); err != nil {
			return outcome, err
		}
		return callResult{messages: 1, payloadBytes: uint64(p.Size), requestBytes: uint64(proto.Size(in)), responseBytes: uint64(proto.Size(out))}, nil
	}
	stream, err := c.conn.NewStream(ctx, &grpc.StreamDesc{
		ClientStreams: method.IsStreamingClient(), ServerStreams: method.IsStreamingServer(),
	}, "/"+service+"/"+name)
	if err != nil {
		return outcome, err
	}
	sent := make(chan time.Time, p.Messages)
	resume := make(chan struct{})
	type sentResult struct {
		bytes uint64
		err   error
	}
	sender := make(chan sentResult, 1)
	go func() {
		var total uint64
		count := p.Messages
		if p.Shape == "download" {
			count = 1
		}
		for sequence := 1; sequence <= count; sequence++ {
			var fields map[string]any
			switch p.Shape {
			case "download":
				fields = map[string]any{"note_id": id}
			case "upload":
				fields = map[string]any{"note_id": id, "sequence": uint32(sequence), "content": []byte(payload)}
			case "duplex":
				fields = map[string]any{"note_id": id, "base_revision": uint32(sequence - 1), "text": payload}
				sent <- time.Now()
			}
			in := message(method.Input(), fields)
			total += uint64(proto.Size(in))
			if err := stream.SendMsg(in); err != nil {
				sender <- sentResult{total, err}
				return
			}
			if p.Mode == "duplex-probe" && sequence == 1 {
				select {
				case <-resume:
				case <-ctx.Done():
					sender <- sentResult{total, ctx.Err()}
					return
				}
			}
		}
		sender <- sentResult{total, stream.CloseSend()}
	}()
	want := p.Messages
	if p.Shape == "upload" {
		want = 1
	}
	var receiveError error
	for sequence := 1; sequence <= want; sequence++ {
		out := dynamicpb.NewMessage(method.Output())
		if err := stream.RecvMsg(out); err != nil {
			receiveError = err
			break
		}
		if sequence == 1 {
			outcome.first = time.Since(start)
			if p.Mode == "duplex-probe" {
				close(resume)
			}
		}
		var expected *dynamicpb.Message
		switch p.Shape {
		case "upload":
			expected = message(method.Output(), map[string]any{"note_id": id, "chunks_received": uint32(p.Messages)})
			f := expected.Descriptor().Fields().ByName("bytes_received")
			expected.Set(f, protoreflect.ValueOfUint64(uint64(p.Messages*p.Size)))
		case "download":
			expected = message(method.Output(), map[string]any{"note_id": id, "revision": uint32(sequence), "summary": payload})
		case "duplex":
			expected = message(method.Output(), map[string]any{"note_id": id, "revision": uint32(sequence), "accepted_text": payload})
			outcome.messageLatency = append(outcome.messageLatency, time.Since(<-sent))
		}
		if !proto.Equal(out, expected) {
			receiveError = invalidResponse{fmt.Sprintf("response message %d differs from the complete expected response", sequence)}
			break
		}
		outcome.messages++
		outcome.payloadBytes += uint64(p.Size)
		outcome.responseBytes += uint64(proto.Size(out))
	}
	if receiveError == nil {
		extra := dynamicpb.NewMessage(method.Output())
		if err := stream.RecvMsg(extra); err != io.EOF {
			if err == nil {
				receiveError = invalidResponse{"extra response after the expected message count"}
			} else {
				receiveError = err
			}
		}
	}
	// The caller cancels the RPC on error, allowing a blocked sender to settle.
	if receiveError != nil {
		cancelStream()
		<-sender
		return outcome, receiveError
	}
	sending := <-sender
	if sending.err != nil {
		return outcome, sending.err
	}
	outcome.requestBytes = sending.bytes
	headers, err := stream.Header()
	if err != nil {
		return outcome, err
	}
	if p.Shape == "upload" {
		outcome.payloadBytes = uint64(p.Messages * p.Size)
	}
	return outcome, validateMetadata(headers, stream.Trailer(), p.Case)
}

func run(p plan) (result, error) {
	r := result{Statuses: map[string]uint64{}, Errors: []string{}, RPCHistogram: histogram{}, MessageHist: histogram{}, FirstHist: histogram{}, ResourceScope: "whole_generator_process",
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0)}
	if p.Concurrency < 1 || p.Messages < 1 || p.Size < 0 || (p.Shape == "unary" && p.Size > 0 && p.Size < 16) ||
		p.Drain <= 0 || (p.Requests == 0 && p.Duration <= 0) {
		return r, fmt.Errorf("invalid finite load plan")
	}
	if !map[string]bool{"unary": true, "upload": true, "download": true, "duplex": true}[p.Shape] ||
		!map[string]bool{"": true, "duplex-probe": true, "cancel": true}[p.Mode] {
		return r, fmt.Errorf("unknown shape or mode")
	}
	data, err := os.ReadFile(p.Descriptor)
	if err != nil {
		return r, err
	}
	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(data, set); err != nil {
		return r, err
	}
	files, err := protodesc.NewFiles(set)
	if err != nil {
		return r, err
	}
	desc, err := files.FindDescriptorByName(service)
	if err != nil {
		return r, err
	}
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if p.CA != "" {
		ca, err := os.ReadFile(p.CA)
		if err != nil {
			return r, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			return r, fmt.Errorf("CA file contains no certificate")
		}
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}))
	}
	connections := &connectionStats{}
	conn, err := grpc.NewClient(p.Endpoint, creds, grpc.WithStatsHandler(connections), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(2<<20), grpc.MaxCallSendMsgSize(2<<20)))
	if err != nil {
		return r, err
	}
	defer conn.Close()
	c := client{conn: conn, methods: desc.(protoreflect.ServiceDescriptor), plan: p}
	ready, cancel := context.WithTimeout(context.Background(), time.Duration(p.Drain*float64(time.Second)))
	defer cancel()
	control := "__bench_state"
	if p.VerifyIdentity {
		control = "__bench_identity"
	}
	r.Identity, err = c.unaryControl(ready, control)
	if err != nil {
		return r, fmt.Errorf("identity RPC: %w", err)
	}
	var identity map[string]any
	if err := json.Unmarshal([]byte(r.Identity), &identity); err != nil {
		return r, fmt.Errorf("invalid identity receipt: %w", err)
	}
	r.TLS = c.tls
	if p.Mode == "cancel" {
		start := time.Now()
		r.Started, r.Attempted = 1, 1
		receipt, err := c.cancelProbe(ready)
		if err != nil {
			r.Invalid = 1
			r.Statuses[codes.Unknown.String()] = 1
			r.Errors = append(r.Errors, err.Error())
		} else {
			r.Cancelled = 1
			r.Statuses[codes.Canceled.String()] = 1
			r.Identity = receipt
		}
		r.Elapsed = time.Since(start).Seconds()
		r.Connections = int(connections.opened.Load())
		return r, nil
	}
	start := time.Now()
	stopStarts := start.Add(time.Duration(p.Duration * float64(time.Second)))
	ctx, stop := context.WithTimeout(context.Background(), time.Duration((p.Duration+p.Drain)*float64(time.Second)))
	defer stop()
	var next atomic.Uint64
	var lock sync.Mutex
	var workers sync.WaitGroup
	for range p.Concurrency {
		workers.Go(func() {
			for {
				if ctx.Err() != nil || (p.Requests == 0 && !time.Now().Before(stopStarts)) {
					return
				}
				number := next.Add(1)
				if p.Requests > 0 && number > p.Requests {
					return
				}
				before := time.Now()
				callCtx, cancelCall := context.WithCancel(ctx)
				work, callErr := c.call(callCtx, number)
				cancelCall()
				elapsed := time.Since(before)
				lock.Lock()
				r.Attempted++
				r.Started++
				code := status.Code(callErr)
				if callErr == nil {
					r.VerifiedOK++
					r.Messages += work.messages
					r.PayloadBytes += work.payloadBytes
					r.RequestBytes += work.requestBytes
					r.ResponseBytes += work.responseBytes
					r.RPCHistogram.add(elapsed)
					if work.first > 0 {
						r.FirstHist.add(work.first)
					}
					for _, latency := range work.messageLatency {
						r.MessageHist.add(latency)
					}
				} else {
					var invalid invalidResponse
					switch {
					case errors.As(callErr, &invalid):
						r.Invalid++
					case code == codes.DeadlineExceeded || errors.Is(callErr, context.DeadlineExceeded):
						r.Deadlines++
					case ctx.Err() != nil:
						r.Unfinished++
					default:
						r.OtherNonOK++
					}
					if len(r.Errors) < 16 {
						r.Errors = append(r.Errors, callErr.Error())
					}
				}
				r.Statuses[code.String()]++
				lock.Unlock()
			}
		})
	}
	workers.Wait()
	r.Elapsed = time.Since(start).Seconds()
	r.Window = min(r.Elapsed, p.Duration)
	if p.Requests > 0 {
		r.Window = r.Elapsed
	}
	r.Drain = r.Elapsed - r.Window
	r.RPCPercentiles = r.RPCHistogram.percentiles()
	r.MessageLatency = r.MessageHist.percentiles()
	r.FirstLatency = r.FirstHist.percentiles()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return r, err
	}
	r.CPUUser = float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	r.CPUSystem = float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
	procStatus, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return r, err
	}
	for _, line := range strings.Split(string(procStatus), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			r.MaxRSSKiB, err = strconv.ParseInt(strings.Fields(line)[1], 10, 64)
			if err != nil {
				return r, err
			}
		}
	}
	if r.MaxRSSKiB <= 0 {
		return r, fmt.Errorf("generator VmHWM unavailable")
	}
	r.Connections = int(connections.opened.Load())
	return r, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: grpc_client PLAN.json")
		os.Exit(2)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer file.Close()
	var p plan
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	r, err := run(p)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
