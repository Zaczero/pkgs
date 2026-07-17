import ws from 'k6/ws';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const echoSuccess = new Rate('bench_echo_success');

export default function () {
  const url = __ENV.WS_URL;
  const nonce = `h2corn-${__VU}-${__ITER}-${Date.now()}`;
  let echoed = false;
  let messages = 0;
  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      socket.send(nonce);
    });

    socket.on('message', function (msg) {
      messages += 1;
      echoed = messages === 1 && msg === nonce;
      socket.close();
    });

    socket.on('error', function (e) {
      console.error('error:', e.error());
    });
  });

  check(res, { 'status is 101': (r) => r && r.status === 101 });
  const exactEcho = echoed && messages === 1;
  echoSuccess.add(exactEcho);
  check(exactEcho, { 'first message exactly echoed nonce': (value) => value });
}
