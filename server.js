// Servidor de Patdera: salas + relé de entradas (lockstep).
// No simula el juego: cada navegador simula igual y el servidor solo reparte las entradas de cada tick.
// Uso: npm install && node server.js   (puerto con PORT=3000, retraso de entrada con DELAY=3)
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 3000;
const DELAY = Math.max(1, +process.env.DELAY || 5); // minimo; el servidor lo sube solo segun el ping de los jugadores
const MAXP = 8, MAXP_TN = 16;
const ALL = ['fire','water','earth','wind','swords','clubs','gold','cups','rabbit','ferret','horse','spider','psu','wifi','monitor','speaker','luz','pombero','jasy','gual','momia','garg','draugr','tupi'];
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
server.on('connection', sock => { try { sock.setNoDelay(true); } catch (e) {} }); // sin algoritmo de Nagle: los paquetes chicos salen al instante

const rooms = new Map();
let nextPid = 1;
const tok = () => Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const TNAMES = ['Los Fénix','Rayo Azul','Marea Roja','Tormenta','Dunas','Cumbre','Abismo','Ceniza','Aurora','Los Lince'];
const shuffle = a => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };

function getRoom(code, mode) {
  let r = rooms.get(code);
  if (!r) { r = { code, mode: mode === 'tn' ? 'tn' : 'ctf', players: new Map(), opts: { bots: true, ts: 3 }, state: 'lobby', host: null, matches: new Map(), nextMid: 1, timer: null, tn: null, idle: null }; rooms.set(code, r); }
  return r;
}
const humans = r => [...r.players.values()];
const inTeam = (r, t) => humans(r).filter(p => p.team === t);
const maxTeam = r => r.mode === 'tn' ? 2 : r.opts.ts;
function freeEl(r, team, prefer) {
  const used = new Set(inTeam(r, team).map(p => p.el));
  if (prefer && !used.has(prefer)) return prefer;
  return ALL.find(e => !used.has(e));
}
function lobbyMsg(r) {
  return { t: 'lobby', mode: r.mode, host: r.host, opts: r.opts, players: humans(r).map(p => ({ pid: p.pid, name: p.name, team: p.team, el: p.el })) };
}
function bcastLobby(r) { const m = lobbyMsg(r); for (const p of r.players.values()) send(p.ws, m); }
function bcast(r, o) { const s = JSON.stringify(o); for (const p of r.players.values()) if (p.ws && p.ws.readyState === 1) p.ws.send(s); }
function bcastM(r, mt, o) { const s = JSON.stringify(o); for (const pid of mt.players) { const p = r.players.get(pid); if (p && p.ws && p.ws.readyState === 1) p.ws.send(s); } }

/* ---------- partidas (una por sala en clasico; varias a la vez en torneo) ---------- */
function mkMatch(r, players) {
  const mt = { id: r.nextMid++, players: new Set(players), state: 'wait', next: 0, inputs: new Map(), hashes: new Map(), pendDrop: [], rejoined: new Set(), dropped: new Set(), start: null, timer: null, teams: null, res: null, mi: -1 };
  r.matches.set(mt.id, mt);
  return mt;
}
function delayFor(r, pids) {
  let worst = 0, jit = 0;
  for (const pid of pids) {
    const p = r.players.get(pid);
    if (p && p.ws && p.ws.rtts && p.ws.rtts.length) { const a = p.ws.rtts; const mx = Math.max(...a), mn = Math.min(...a); worst = Math.max(worst, mx); jit = Math.max(jit, mx - mn); }
  }
  // colchon contra la variacion del ping (jitter): menos tirones a cambio de unos ms de retraso
  return { worst, delay: Math.min(24, Math.max(DELAY, Math.ceil((worst * 1.15 + jit * 1.5 + 55) / 16.7))) };
}
function launch(r, mt, size, slots, extra) {
  const d = delayFor(r, mt.players);
  console.log('Sala ' + r.code + ' partida ' + mt.id + ': ping maximo ' + d.worst + ' ms -> retraso de entrada ' + d.delay + ' ticks');
  mt.start = Object.assign({ t: 'start', mid: mt.id, seed: Math.floor(Math.random() * 4294967295), map: Math.floor(Math.random() * 4), ts: size, delay: d.delay, slots }, extra || {});
  bcastM(r, mt, mt.start);
  mt.timer = setTimeout(() => goMatch(r, mt), 25000);
}
function startMatch(r) {
  const ts = r.opts.ts, fill = r.opts.bots;
  const slots = [];
  const c0 = inTeam(r, 0).length, c1 = inTeam(r, 1).length;
  if (!fill && (c0 < 1 || c1 < 1)) return 'Sin bots hace falta al menos 1 jugador en cada equipo';
  const size = fill ? ts : Math.max(c0, c1);
  for (let t = 0; t < 2; t++) {
    const hs = inTeam(r, t).sort((a, b) => a.pid - b.pid);
    const used = new Set(hs.map(p => p.el));
    const team = hs.map(p => ({ team: t, el: p.el, pid: p.pid, name: p.name }));
    while (fill && team.length < size) {
      const pool = ALL.filter(e => !used.has(e));
      const e = pool[Math.floor(Math.random() * pool.length)];
      used.add(e); team.push({ team: t, el: e, pid: null });
    }
    team.forEach((s, i) => { s.last = i === team.length - 1 ? 1 : 0; slots.push(s); });
  }
  r.state = 'run';
  const mt = mkMatch(r, humans(r).map(p => p.pid));
  launch(r, mt, size, slots);
  return null;
}
function goMatch(r, mt) {
  if (mt.state !== 'wait') return;
  clearTimeout(mt.timer);
  mt.state = 'play';
  const absent = [];
  for (const pid of mt.players) if (!mt.rejoined.has(pid)) { mt.dropped.add(pid); absent.push(pid); }
  bcastM(r, mt, { t: 'go', absent });
  if (![...mt.players].some(pid => !mt.dropped.has(pid))) endMatchNoPlayers(r, mt);
}
function activeM(r, mt) { return [...mt.players].filter(pid => !mt.dropped.has(pid)).map(pid => r.players.get(pid)).filter(Boolean); }
function tryFrames(r, mt) {
  if (mt.state !== 'play') return;
  for (;;) {
    const act = activeM(r, mt);
    if (!act.length) { endMatchNoPlayers(r, mt); return; }
    const frame = {};
    for (const p of act) {
      const q = mt.inputs.get(p.pid);
      const i = q && q.get(mt.next);
      if (!i) return;
      frame[p.pid] = i;
    }
    for (const p of act) mt.inputs.get(p.pid).delete(mt.next);
    const msg = { t: 'f', n: mt.next, in: frame };
    if (mt.pendDrop.length) { msg.drop = mt.pendDrop; mt.pendDrop = []; }
    bcastM(r, mt, msg);
    mt.next++;
  }
}
function endMatchNoPlayers(r, mt) {
  clearTimeout(mt.timer);
  if (r.mode !== 'tn') { closeRoom(r); return; }
  if (mt.state === 'done') return;
  // nadie quedó conectado: define el servidor (gana el equipo con mas jugadores conectados, o azar)
  if (mt.res == null) mt.res = Math.random() < .5 ? 0 : 1;
  matchDone(r, mt);
}
function closeRoom(r) { clearTimeout(r.timer); clearTimeout(r.idle); for (const m of r.matches.values()) clearTimeout(m.timer); rooms.delete(r.code); }

/* ---------- torneo online ---------- */
function tnTeamHumans(r, t) { return inTeam(r, t).filter(p => p.ws && p.ws.readyState === 1); }
function tnBracketMsg(r) {
  const T = r.tn;
  const now = Date.now();
  return { t: 'tnb', teams: T.teams.map((tm, i) => ({ n: tm.n, ps: inTeam(r, i).map(p => ({ pid: p.pid, name: p.name, el: p.el })) })), rounds: T.rounds, r: T.r, st: T.st, at: T.at ? Math.max(0, T.at - now) : 0, champ: T.champ, mids: T.mids };
}
function bcastTn(r) { bcast(r, tnBracketMsg(r)); }
function startTournament(r) {
  const hs = humans(r);
  if (!hs.length) return 'No hay jugadores';
  const names = shuffle(TNAMES);
  const teams = [];
  let bi = 0;
  for (let i = 0; i < 8; i++) {
    const ps = inTeam(r, i);
    teams.push({ n: ps.length ? (ps.length > 1 ? ps.map(p => p.name).join(' & ') : 'Equipo de ' + ps[0].name) : names[bi++ % names.length] });
  }
  const o = shuffle([0, 1, 2, 3, 4, 5, 6, 7]);
  r.tn = { teams, rounds: [[[o[0], o[1], null], [o[2], o[3], null], [o[4], o[5], null], [o[6], o[7], null]]], r: 0, st: 'between', at: Date.now() + 20000, champ: null, mids: {} };
  r.state = 'tn';
  bcast(r, { t: 'tn0' });
  r.timer = setTimeout(() => startRound(r), 20000);
  return null;
}
function startRound(r) {
  const T = r.tn; if (!T || T.st === 'done') return;
  T.st = 'play'; T.at = 0; T.mids = {};
  const fin = T.r === 2, rn = ['Cuartos de final', 'Semifinal', 'Final'][T.r];
  T.rounds[T.r].forEach((m, mi) => {
    if (m[2] != null) return;
    const [a, b] = m, ha = tnTeamHumans(r, a), hb = tnTeamHumans(r, b);
    if (!ha.length && !hb.length) { m[2] = Math.random() < .5 ? a : b; return; }
    const mt = mkMatch(r, [...ha, ...hb].map(p => p.pid)); mt.teams = [a, b]; mt.mi = mi;
    T.mids[mi] = mt.id;
    for (const pid of mt.players) { const p = r.players.get(pid); if (p) p.inMatch = true; }
    startDraft(r, mt, { rn, final: fin, names: [T.teams[a].n, T.teams[b].n], hs: [ha, hb].map(h => h.slice().sort((x, y) => x.pid - y.pid)) });
  });
  bcastTn(r);
  checkRound(r);
}
/* ---------- baneos y picks (draft) de cada partida del torneo ---------- */
const DRAFT_MS = 25000, BOT_MS = 1100;
function drAvail(D) { const used = new Set([...D.bans.filter(Boolean), ...D.picks[0], ...D.picks[1]]); return ALL.filter(e => !used.has(e)); }
function drActor(D) {
  const q = D.seq[D.i]; if (!q) return null;
  const side = q[1], h = D.hs[side];
  const p = q[0] === 'ban' ? h[0] : h[D.picks[side].length];
  return p ? p.pid : null;
}
function drMsg(D, mt) {
  const q = D.seq[D.i];
  return { t: 'dr', mid: mt.id, rn: D.rn, final: D.final, names: D.names, hs: D.hs.map(h => h.map(p => ({ pid: p.pid, name: p.name }))), bans: D.bans, picks: D.picks, seq: D.seq, i: D.i, actor: drActor(D), left: D.left > 0 ? Math.max(0, D.left - Date.now()) : 0, last: D.last || null, done: D.i >= D.seq.length };
}
function drStep(r, mt) {
  const D = mt.dr; if (!D) return;
  clearTimeout(D.timer);
  if (D.i >= D.seq.length) { D.left = 0; bcastM(r, mt, drMsg(D, mt)); D.timer = setTimeout(() => drFinish(r, mt), 1800); return; }
  const actor = drActor(D);
  const ms = actor == null ? BOT_MS : DRAFT_MS;
  D.left = actor == null ? 0 : Date.now() + ms;
  bcastM(r, mt, drMsg(D, mt));
  D.timer = setTimeout(() => { const a = drAvail(D); drApply(r, mt, a[Math.floor(Math.random() * a.length)]); }, ms);
}
function drApply(r, mt, el) {
  const D = mt.dr; if (!D || D.i >= D.seq.length) return;
  const q = D.seq[D.i];
  if (q[0] === 'ban') { D.bans[q[1]] = el; D.last = 'b' + q[1]; }
  else { D.last = 'p' + q[1] + D.picks[q[1]].length; D.picks[q[1]].push(el); }
  D.i++;
  drStep(r, mt);
}
function drFinish(r, mt) {
  const D = mt.dr; if (!D) return;
  const slots = [];
  for (let side = 0; side < 2; side++) {
    for (let j = 0; j < 2; j++) {
      const h = D.hs[side][j];
      slots.push({ team: side, el: D.picks[side][j], pid: h ? h.pid : null, name: h ? h.name : undefined, last: j === 1 ? 1 : 0 });
    }
  }
  mt.dr = null; mt.state = 'wait';
  launch(r, mt, 2, slots, { tn: { rn: D.rn, final: D.final, names: D.names, ban: D.bans } });
}
function startDraft(r, mt, info) {
  const f = r.tn.r % 2 === 0 ? 0 : 1, sc = 1 - f;
  mt.state = 'draft';
  mt.dr = Object.assign({ bans: [null, null], picks: [[], []], seq: [['ban', 0], ['ban', 1], ['pick', f], ['pick', sc], ['pick', sc], ['pick', f]], i: 0, left: 0, timer: null, last: null }, info);
  drStep(r, mt);
}

function matchDone(r, mt) {
  if (mt.state === 'done') return;
  mt.state = 'done'; clearTimeout(mt.timer); if (mt.dr) { clearTimeout(mt.dr.timer); mt.dr = null; }
  for (const pid of mt.players) { const p = r.players.get(pid); if (p) p.inMatch = false; }
  const T = r.tn;
  if (T && mt.teams && mt.res != null) { const m = T.rounds[T.r][mt.mi]; if (m && m[2] == null) m[2] = mt.teams[mt.res]; }
  setTimeout(() => r.matches.delete(mt.id), 5000);
  if (T) { bcastTn(r); checkRound(r); }
}
function checkRound(r) {
  const T = r.tn; if (!T || T.st !== 'play') return;
  if (!T.rounds[T.r].every(m => m[2] != null)) return;
  if (T.r === 2) { T.st = 'done'; T.champ = T.rounds[2][0][2]; T.at = 0; bcastTn(r); idleSoon(r); return; }
  const q = T.rounds[T.r]; const nx = [];
  for (let i = 0; i < q.length; i += 2) nx.push([q[i][2], q[i + 1][2], null]);
  T.r++; T.rounds[T.r] = nx; T.st = 'between'; T.at = Date.now() + 14000;
  bcastTn(r);
  r.timer = setTimeout(() => startRound(r), 14000);
}
function idleSoon(r) { clearTimeout(r.idle); r.idle = setTimeout(() => closeRoom(r), 10 * 60 * 1000); }
function anyConnected(r) { return humans(r).some(p => p.ws && p.ws.readyState === 1); }

wss.on('connection', ws => {
  ws.missed = 0; ws.rtts = [];
  ws.on('pong', () => { ws.missed = 0; ws.rtts.push(Date.now() - ws.pt); if (ws.rtts.length > 8) ws.rtts.shift(); });
  let room = null, player = null;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'join') {
      const code = String(m.room || 'sala1').slice(0, 12);
      const r = getRoom(code, m.mode);
      if (r.state !== 'lobby') return send(ws, { t: 'err', m: 'La partida de esa sala ya empezó' });
      if (r.players.size >= (r.mode === 'tn' ? MAXP_TN : MAXP)) return send(ws, { t: 'err', m: 'Sala llena' });
      let team;
      if (r.mode === 'tn') {
        const order = [0, 1, 2, 3, 4, 5, 6, 7].filter(t => inTeam(r, t).length < 2);
        if (!order.length) return send(ws, { t: 'err', m: 'Sala llena' });
        team = order.find(t => inTeam(r, t).length === 0); if (team == null) team = order[0];
      } else team = inTeam(r, 0).length <= inTeam(r, 1).length ? 0 : 1;
      player = { pid: nextPid++, name: String(m.name || 'Jugador').slice(0, 14), team, el: null, ws, token: tok(), inMatch: false };
      player.el = freeEl(r, team);
      r.players.set(player.pid, player); room = r;
      if (r.host == null) r.host = player.pid;
      send(ws, { t: 'welcome', pid: player.pid, token: player.token, mode: r.mode });
      bcastLobby(r);
    } else if (m.t === 'rejoin') {
      const r = rooms.get(String(m.room || ''));
      if (!r) return send(ws, { t: 'err', m: 'La sala ya no existe' });
      const p = humans(r).find(x => x.token === m.token);
      if (!p) return send(ws, { t: 'err', m: 'Jugador desconocido' });
      p.ws = ws; player = p; room = r;
      clearTimeout(r.idle);
      if (m.mid) {
        const mt = r.matches.get(m.mid);
        if (!mt || !mt.players.has(p.pid)) return send(ws, { t: 'err', m: 'Esa partida ya terminó' });
        mt.rejoined.add(p.pid);
        if (mt.state === 'wait' && [...mt.players].every(pid => mt.rejoined.has(pid))) goMatch(r, mt);
        else if (mt.state === 'play') send(ws, { t: 'err', m: 'La partida ya empezó' });
      } else if (r.mode === 'tn' && r.tn) {
        send(ws, { t: 'welcome', pid: p.pid, token: p.token, mode: 'tn' });
        send(ws, tnBracketMsg(r));
        for (const mt of r.matches.values()) if (mt.dr && mt.players.has(p.pid)) send(ws, drMsg(mt.dr, mt));
      }
    } else if (!room || !player) {
      return;
    } else if (m.t === 'set' && (room.state === 'lobby' || (room.mode === 'tn' && room.state === 'tn' && !player.inMatch))) {
      const r = room;
      if (r.state === 'lobby' && Number.isInteger(m.team) && m.team >= 0 && m.team < (r.mode === 'tn' ? 8 : 2)) {
        if (m.team !== player.team) {
          if (inTeam(r, m.team).length >= maxTeam(r)) return send(ws, { t: 'err', m: 'Ese equipo está lleno' });
          player.team = m.team; player.el = freeEl(r, m.team, player.el);
        }
      }
      if (ALL.includes(m.el)) {
        if (inTeam(r, player.team).some(p => p !== player && p.el === m.el)) return send(ws, { t: 'err', m: 'Ese personaje ya está en tu equipo' });
        player.el = m.el;
      }
      if (r.state === 'lobby') bcastLobby(r); else bcastTn(r);
    } else if (m.t === 'opts' && room.state === 'lobby' && room.host === player.pid && room.mode === 'ctf') {
      const r = room;
      if (typeof m.bots === 'boolean') r.opts.bots = m.bots;
      if (m.ts === 3 || m.ts === 4) {
        if (inTeam(r, 0).length > m.ts || inTeam(r, 1).length > m.ts) send(ws, { t: 'err', m: 'Hay más jugadores en un equipo que ese tamaño' });
        else r.opts.ts = m.ts;
      }
      bcastLobby(r);
    } else if (m.t === 'start' && room.state === 'lobby' && room.host === player.pid) {
      const err = room.mode === 'tn' ? startTournament(room) : startMatch(room);
      if (err) send(ws, { t: 'err', m: err });
    } else if (m.t === 'i') {
      const mt = room.matches.get(m.mid || 0) || (room.mode === 'ctf' ? [...room.matches.values()][0] : null);
      if (!mt || mt.state !== 'play' || !mt.players.has(player.pid)) return;
      if (!Number.isInteger(m.n) || m.n < mt.next || m.n > mt.next + 64 || !m.i) return;
      let q = mt.inputs.get(player.pid); if (!q) { q = new Map(); mt.inputs.set(player.pid, q); }
      q.set(m.n, m.i);
      tryFrames(room, mt);
    } else if (m.t === 'h') {
      const mt = room.matches.get(m.mid || 0) || (room.mode === 'ctf' ? [...room.matches.values()][0] : null);
      if (!mt || mt.state !== 'play') return;
      let e = mt.hashes.get(m.n); if (!e) { e = new Map(); mt.hashes.set(m.n, e); }
      e.set(player.pid, m.h);
      if (activeM(room, mt).every(p => e.has(p.pid))) {
        const vals = [...e.values()];
        if (vals.some(v => v !== vals[0])) bcastM(room, mt, { t: 'desync', n: m.n });
        mt.hashes.delete(m.n);
      }
    } else if (m.t === 'dpick' && room.mode === 'tn') {
      const mt = room.matches.get(m.mid);
      if (!mt || !mt.dr || !mt.players.has(player.pid) || mt.dr.i >= mt.dr.seq.length) return;
      if (drActor(mt.dr) !== player.pid) return send(ws, { t: 'err', m: 'No es tu turno' });
      if (!drAvail(mt.dr).includes(m.el)) return send(ws, { t: 'err', m: 'Ese personaje no está disponible' });
      drApply(room, mt, m.el);
    } else if (m.t === 'res' && room.mode === 'tn') {
      const mt = room.matches.get(m.mid);
      if (!mt || !mt.players.has(player.pid) || mt.state === 'done' || (m.w !== 0 && m.w !== 1)) return;
      if (mt.res == null) mt.res = m.w;
      matchDone(room, mt);
    }
  });
  ws.on('close', () => {
    if (!room || !player || player.ws !== ws) return;
    const r = room;
    if (r.state === 'lobby') {
      r.players.delete(player.pid);
      if (r.host === player.pid) r.host = r.players.size ? [...r.players.keys()][0] : null;
      if (!r.players.size) closeRoom(r); else bcastLobby(r);
    } else {
      player.ws = null;
      for (const mt of r.matches.values()) {
        if (mt.state === 'play' && mt.players.has(player.pid) && !mt.dropped.has(player.pid)) { mt.dropped.add(player.pid); mt.pendDrop.push(player.pid); tryFrames(r, mt); }
      }
      if (r.mode === 'tn' && !anyConnected(r)) { clearTimeout(r.idle); r.idle = setTimeout(() => { if (!anyConnected(r)) closeRoom(r); }, 120000); }
    }
  });
});
setInterval(() => { for (const ws of wss.clients) { if (ws.missed > 10) { ws.terminate(); continue; } ws.missed++; ws.pt = Date.now(); try { ws.ping(); } catch (e) {} } }, 2000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('Patdera - servidor listo en el puerto ' + PORT);
  console.log('  En esta PC:        http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list || []) if (i.family === 'IPv4' && !i.internal) console.log('  Para tu LAN:       http://' + i.address + ':' + PORT + '   (servidor del juego: ws://' + i.address + ':' + PORT + ')');
});
