const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const https = require('https');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'cache');
const CACHE_FILE = path.join(DATA_DIR, 'imdb-top500.json');
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

function fetchBuffer(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return resolve(fetchBuffer(res.headers.location));
      }
      if (res.statusCode !== 200) {
        reject(new Error(`Failed to fetch ${url}: ${res.statusCode}`));
        res.resume();
        return;
      }
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    }).on('error', reject);
  });
}

async function fetchAndGunzip(url) {
  const buf = await fetchBuffer(url);
  return zlib.gunzipSync(buf).toString('utf-8');
}

function parseTsv(text) {
  const lines = text.trim().split('\n');
  const headers = lines.shift().split('\t');
  return lines.map((line) => {
    const cols = line.split('\t');
    const obj = {};
    for (let i = 0; i < headers.length; i += 1) obj[headers[i]] = cols[i];
    return obj;
  });
}

function buildRankedMovies(basicsRows, ratingsRows) {
  const basicsMap = new Map();
  for (const row of basicsRows) {
    if (row.titleType !== 'movie') continue;
    if (row.isAdult === '1') continue;
    if (!row.primaryTitle || row.primaryTitle === '\\N') continue;
    basicsMap.set(row.tconst, row);
  }

  const movies = [];
  for (const row of ratingsRows) {
    const basic = basicsMap.get(row.tconst);
    if (!basic) continue;

    const averageRating = Number(row.averageRating);
    const numVotes = Number(row.numVotes);
    const year = basic.startYear && basic.startYear !== '\\N' ? Number(basic.startYear) : null;
    const runtime = basic.runtimeMinutes && basic.runtimeMinutes !== '\\N' ? Number(basic.runtimeMinutes) : null;
    const genres = basic.genres && basic.genres !== '\\N' ? basic.genres.split(',') : [];

    if (!Number.isFinite(averageRating) || !Number.isFinite(numVotes)) continue;
    if (numVotes < 2500) continue;

    const score = averageRating * Math.log10(numVotes + 1);

    movies.push({
      id: basic.tconst,
      title: basic.primaryTitle,
      originalTitle: basic.originalTitle && basic.originalTitle !== '\\N' ? basic.originalTitle : basic.primaryTitle,
      year,
      runtime,
      genres,
      averageRating: Number(averageRating.toFixed(1)),
      numVotes,
      score: Number(score.toFixed(4))
    });
  }

  movies.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.averageRating !== a.averageRating) return b.averageRating - a.averageRating;
    if (b.numVotes !== a.numVotes) return b.numVotes - a.numVotes;
    return (a.year || 0) - (b.year || 0);
  });

  return movies.slice(0, 500).map((movie, index) => ({
    ...movie,
    rank: index + 1
  }));
}

async function loadMovies() {
  try {
    const cached = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
    if (cached && cached.generatedAt && Array.isArray(cached.movies)) {
      const age = Date.now() - new Date(cached.generatedAt).getTime();
      if (Number.isFinite(age) && age < CACHE_TTL_MS && cached.movies.length >= 500) {
        return cached.movies;
      }
    }
  } catch (_) {
    // cache miss
  }

  const [basicsText, ratingsText] = await Promise.all([
    fetchAndGunzip('https://datasets.imdbws.com/title.basics.tsv.gz'),
    fetchAndGunzip('https://datasets.imdbws.com/title.ratings.tsv.gz')
  ]);

  const basicsRows = parseTsv(basicsText);
  const ratingsRows = parseTsv(ratingsText);
  const movies = buildRankedMovies(basicsRows, ratingsRows);

  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CACHE_FILE, JSON.stringify({
    generatedAt: new Date().toISOString(),
    movies
  }, null, 2));

  return movies;
}

const rooms = new Map();
let topMovies = [];

function getRoom(roomCode) {
  if (!rooms.has(roomCode)) {
    rooms.set(roomCode, {
      code: roomCode,
      users: new Map(),
      picks: new Map(),
      likes: new Map(),
      dislikes: new Map(),
      createdAt: Date.now()
    });
  }
  return rooms.get(roomCode);
}

function roomSnapshot(room) {
  return {
    code: room.code,
    userCount: room.users.size,
    users: Array.from(room.users.values()).map((u) => ({ id: u.id, name: u.name })),
    totalMovies: topMovies.length
  };
}

function currentMovieForUser(room, socketId) {
  const user = room.users.get(socketId);
  if (!user) return null;
  const index = user.index || 0;
  return topMovies[index] || null;
}

function emitRoomState(room) {
  const payload = roomSnapshot(room);
  for (const user of room.users.values()) {
    io.to(user.id).emit('room:update', {
      ...payload,
      yourIndex: user.index || 0,
      yourMovie: currentMovieForUser(room, user.id)
    });
  }
}

function advanceUser(room, socketId) {
  const user = room.users.get(socketId);
  if (!user) return;
  user.index = (user.index || 0) + 1;

  const nextMovie = currentMovieForUser(room, socketId);
  io.to(socketId).emit('movie:next', {
    movie: nextMovie,
    finished: !nextMovie
  });

  if (!nextMovie) {
    io.to(socketId).emit('deck:finished', {
      matched: Array.from(room.picks.entries())
        .filter(([, voters]) => voters.size >= 2)
        .map(([movieId]) => movieId)
    });
  }
  emitRoomState(room);
}

function ensureMovieSeen(room, movieId) {
  if (!room.picks.has(movieId)) room.picks.set(movieId, new Set());
}

io.on('connection', (socket) => {
  socket.on('room:join', ({ roomCode, name }) => {
    const code = String(roomCode || '').trim().toUpperCase().slice(0, 12) || 'DEMO';
    const nick = String(name || '').trim().slice(0, 24) || `User-${socket.id.slice(0, 4)}`;
    const room = getRoom(code);

    room.users.set(socket.id, {
      id: socket.id,
      name: nick,
      index: 0
    });

    socket.join(code);
    socket.data.roomCode = code;

    const current = currentMovieForUser(room, socket.id);
    socket.emit('room:joined', {
      roomCode: code,
      name: nick,
      movie: current,
      totalMovies: topMovies.length,
      userCount: room.users.size
    });

    emitRoomState(room);
    io.to(code).emit('room:announcement', {
      message: `${nick} joined room ${code}`
    });
  });

  socket.on('movie:vote', ({ movieId, vote }) => {
    const roomCode = socket.data.roomCode;
    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room || !room.users.has(socket.id)) return;

    const movie = topMovies.find((m) => m.id === movieId);
    if (!movie) return;

    ensureMovieSeen(room, movieId);

    const votedYes = vote === 'like';
    const targetSet = votedYes ? room.likes : room.dislikes;
    const opposite = votedYes ? room.dislikes : room.likes;
    if (opposite.has(movieId)) opposite.delete(movieId);

    targetSet.set(movieId, targetSet.get(movieId) || new Set());
    targetSet.get(movieId).add(socket.id);
    room.picks.get(movieId).add(socket.id);

    const likedBy = room.likes.get(movieId) || new Set();
    const user = room.users.get(socket.id);

    if (likedBy.size >= 2) {
      io.to(roomCode).emit('match', {
        movie,
        by: user.name
      });
    }

    io.to(roomCode).emit('vote:update', {
      movieId,
      likes: Array.from(room.likes.get(movieId) || []),
      dislikes: Array.from(room.dislikes.get(movieId) || [])
    });

    advanceUser(room, socket.id);
  });

  socket.on('disconnect', () => {
    const roomCode = socket.data.roomCode;
    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room) return;

    room.users.delete(socket.id);

    for (const set of room.likes.values()) set.delete(socket.id);
    for (const set of room.dislikes.values()) set.delete(socket.id);
    for (const set of room.picks.values()) set.delete(socket.id);

    if (room.users.size === 0) {
      rooms.delete(roomCode);
    } else {
      emitRoomState(room);
      io.to(roomCode).emit('room:announcement', {
        message: 'A user left the room.'
      });
    }
  });
});

app.get('/api/health', (_req, res) => {
  res.json({ ok: true, movies: topMovies.length });
});

app.get('/api/movies', (_req, res) => {
  res.json(topMovies);
});

async function main() {
  try {
    topMovies = await loadMovies();
    console.log(`Loaded ${topMovies.length} IMDb movies`);
  } catch (err) {
    console.error('Failed to load IMDb data:', err);
    topMovies = [];
  }

  server.listen(PORT, () => {
    console.log(`Movie Swipe Club running on http://localhost:${PORT}`);
  });
}

main();
