// Servidor de Elemental CTF: salas + relé de entradas (lockstep).
// No simula el juego: cada navegador simula igual y el servidor solo reparte las entradas de cada tick.
// Uso: npm install && node server.js   (puerto con PORT=3000, retraso de entrada con DELAY=3)
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 3000;
const DELAY = Math.max(1, +process.env.DELAY || 3);
const MAXP = 8;
const ALL = ['fire','water','earth','wind','swords','clubs','gold','cups','rabbit','ferret','horse','spider','psu','wifi','monitor','speaker'];
const STATIC = { '/': 'index.html', '/index.html': 'index.html', '/three.min.js': 'three.min.js' };
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript' };

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.end('ok'); return; }
  const f = STATIC[url];
  if (!f) { res.statusCode = 404; res.end('No encontrado'); return; }
  fs.readFile(path.join(__dirname, f), (err, data) => {
    if (err) { res.statusCode = 404; res.end('Falta ' + f); return; }
    res.setHeader('Content-Type', TYPES[path.extname(f)] || 'application/octet-stream');
    res.end(data);
  });
});
const wss = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 64 * 1024 });

const rooms = new Map();
let nextPid = 1;
const tok = () => Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

function getRoom(code) {
  let r = rooms.get(code);
  if (!r) { r = { code, players: new Map(), opts: { bots: true, ts: 3 }, state: 'lobby', host: null, timer: null }; rooms.set(code, r); }
  return r;
}
const humans = r => [...r.players.values()];
const inTeam = (r, t) => humans(r).filter(p => p.team === t);
function freeEl(r, team, prefer) {
  const used = new Set(inTeam(r, team).map(p => p.el));
  if (prefer && !used.has(prefer)) return prefer;
  return ALL.find(e => !used.has(e));
}
function lobbyMsg(r) {
  return { t: 'lobby', host: r.host, opts: r.opts, players: humans(r).map(p => ({ pid: p.pid, name: p.name, team: p.team, el: p.el })) };
}
function bcastLobby(r) { const m = lobbyMsg(r); for (const p of r.players.values()) send(p.ws, m); }
function bcast(r, o) { const s = JSON.stringify(o); for (const p of r.players.values()) if (p.ws && p.ws.readyState === 1) p.ws.send(s); }

function startMatch(r) {
  const ts = r.opts.ts, fill = r.opts.bots;
  const slots = [];
  const c0 = inTeam(r, 0).length, c1 = inTeam(r, 1).length;
  if (!fill && (c0 < 1 || c1 < 1)) return 'Sin bots hace falta al menos 1 jugador en cada equipo';
  const size = fill ? ts : Math.max(c0, c1);
  for (let t = 0; t < 2; t++) {
    const hs = inTeam(r, t).sort((a, b) => a.pid - b.pid);
    const used = new Set(hs.map(p => p.el));
    const team = hs.map(p => ({ team: t, el: p.el, pid: p.pid }));
    while (fill && team.length < size) {
      const pool = ALL.filter(e => !used.has(e));
      const e = pool[Math.floor(Math.random() * pool.length)];
      used.add(e); team.push({ team: t, el: e, pid: null });
    }
    team.forEach((s, i) => { s.last = i === team.length - 1 ? 1 : 0; slots.push(s); });
  }
  r.state = 'wait';
  r.start = { t: 'start', seed: Math.floor(Math.random() * 4294967295), map: Math.floor(Math.random() * 3), ts: size, delay: DELAY, slots };
  for (const p of r.players.values()) { p.rejoined = false; p.dropped = false; }
  bcast(r, r.start);
  r.timer = setTimeout(() => goMatch(r), 25000);
  return null;
}
function goMatch(r) {
  if (r.state !== 'wait') return;
  clearTimeout(r.timer);
  r.state = 'play'; r.next = 0; r.inputs = new Map(); r.hashes = new Map(); r.pendDrop = [];
  const absent = [];
  for (const p of r.players.values()) if (!p.rejoined) { p.dropped = true; absent.push(p.pid); }
  bcast(r, { t: 'go', absent });
  if (!humans(r).some(p => !p.dropped)) closeRoom(r);
}
function active(r) { return humans(r).filter(p => !p.dropped); }
function tryFrames(r) {
  if (r.state !== 'play') return;
  for (;;) {
    const act = active(r);
    if (!act.length) { closeRoom(r); return; }
    const frame = {};
    for (const p of act) {
      const q = r.inputs.get(p.pid);
      const i = q && q.get(r.next);
      if (!i) return;
      frame[p.pid] = i;
    }
    for (const p of act) r.inputs.get(p.pid).delete(r.next);
    const msg = { t: 'f', n: r.next, in: frame };
    if (r.pendDrop.length) { msg.drop = r.pendDrop; r.pendDrop = []; }
    bcast(r, msg);
    r.next++;
  }
}
function closeRoom(r) { clearTimeout(r.timer); rooms.delete(r.code); }

wss.on('connection', ws => {
  ws.alive = true; ws.on('pong', () => ws.alive = true);
  let room = null, player = null;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join') {
      const code = String(m.room || 'sala1').slice(0, 12);
      const r = getRoom(code);
      if (r.state !== 'lobby') return send(ws, { t: 'err', m: 'La partida de esa sala ya empezó' });
      if (r.players.size >= MAXP) return send(ws, { t: 'err', m: 'Sala llena' });
      const team = inTeam(r, 0).length <= inTeam(r, 1).length ? 0 : 1;
      player = { pid: nextPid++, name: String(m.name || 'Jugador').slice(0, 14), team, el: null, ws, token: tok(), rejoined: false, dropped: false };
      player.el = freeEl(r, team);
      r.players.set(player.pid, player); room = r;
      if (r.host == null) r.host = player.pid;
      send(ws, { t: 'welcome', pid: player.pid, token: player.token });
      bcastLobby(r);
    } else if (m.t === 'rejoin') {
      const r = rooms.get(String(m.room || ''));
      if (!r) return send(ws, { t: 'err', m: 'La sala ya no existe' });
      const p = humans(r).find(x => x.token === m.token);
      if (!p) return send(ws, { t: 'err', m: 'Jugador desconocido' });
      p.ws = ws; p.rejoined = true; player = p; room = r;
      if (r.state === 'wait' && humans(r).every(x => x.rejoined)) goMatch(r);
    } else if (!room || !player) {
      return;
    } else if (m.t === 'set' && room.state === 'lobby') {
      const r = room;
      if (m.team === 0 || m.team === 1) {
        if (m.team !== player.team) {
          if (inTeam(r, m.team).length >= r.opts.ts) return send(ws, { t: 'err', m: 'Ese equipo está lleno' });
          player.team = m.team; player.el = freeEl(r, m.team, player.el);
        }
      }
      if (ALL.includes(m.el)) {
        if (inTeam(r, player.team).some(p => p !== player && p.el === m.el)) return send(ws, { t: 'err', m: 'Ese personaje ya está en tu equipo' });
        player.el = m.el;
      }
      bcastLobby(r);
    } else if (m.t === 'opts' && room.state === 'lobby' && room.host === player.pid) {
      const r = room;
      if (typeof m.bots === 'boolean') r.opts.bots = m.bots;
      if (m.ts === 3 || m.ts === 4) {
        if (inTeam(r, 0).length > m.ts || inTeam(r, 1).length > m.ts) send(ws, { t: 'err', m: 'Hay más jugadores en un equipo que ese tamaño' });
        else r.opts.ts = m.ts;
      }
      bcastLobby(r);
    } else if (m.t === 'start' && room.state === 'lobby' && room.host === player.pid) {
      const err = startMatch(room);
      if (err) send(ws, { t: 'err', m: err });
    } else if (m.t === 'i' && room.state === 'play') {
      if (!Number.isInteger(m.n) || m.n < room.next || m.n > room.next + 64 || !m.i) return;
      let q = room.inputs.get(player.pid); if (!q) { q = new Map(); room.inputs.set(player.pid, q); }
      q.set(m.n, m.i);
      tryFrames(room);
    } else if (m.t === 'h' && room.state === 'play') {
      let e = room.hashes.get(m.n); if (!e) { e = new Map(); room.hashes.set(m.n, e); }
      e.set(player.pid, m.h);
      if (active(room).every(p => e.has(p.pid))) {
        const vals = [...e.values()];
        if (vals.some(v => v !== vals[0])) bcast(room, { t: 'desync', n: m.n });
        room.hashes.delete(m.n);
      }
    }
  });
  ws.on('close', () => {
    if (!room || !player || player.ws !== ws) return;
    const r = room;
    if (r.state === 'lobby') {
      r.players.delete(player.pid);
      if (r.host === player.pid) r.host = r.players.size ? [...r.players.keys()][0] : null;
      if (!r.players.size) closeRoom(r); else bcastLobby(r);
    } else if (r.state === 'play') {
      if (!player.dropped) { player.dropped = true; r.pendDrop.push(player.pid); }
      tryFrames(r);
    }
  });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (e) {} } }, 30000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('Elemental CTF - servidor listo en el puerto ' + PORT);
  console.log('  En esta PC:        http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list || []) if (i.family === 'IPv4' && !i.internal) console.log('  Para tu LAN:       http://' + i.address + ':' + PORT + '   (servidor del juego: ws://' + i.address + ':' + PORT + ')');
});
