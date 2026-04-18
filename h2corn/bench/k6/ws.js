import ws from 'k6/ws';
import { check } from 'k6';

export default function () {
  const url = __ENV.WS_URL;
  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', function () {
      socket.send('Hello');
    });

    socket.on('message', function (msg) {
      if (msg === 'Hello') {
        socket.close();
      }
    });

    socket.on('error', function (e) {
      console.error('error:', e.error());
    });
  });

  check(res, { 'status is 101': (r) => r && r.status === 101 });
}
