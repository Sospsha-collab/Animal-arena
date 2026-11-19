
import { DurableObject } from 'cloudflare:workers';

const okCode = code => /^[A-HJ-NP-Z2-9]{6}$/.test(code);
const send = (socket, message) => {
  try { socket.send(JSON.stringify(message)); } catch {}
};
const safeHero = n => Number.isInteger(n) && n >= 0 && n < 120 ? n : 0;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health')
      return Response.json(
        { ok: true, service: 'animal-arena-relay', version: 2 },
        { headers: { 'Access-Control-Allow-Origin': '*' } }
      );

    if (url.pathname !== '/ws' ||
        request.headers.get('Upgrade')?.toLowerCase() !== 'websocket')
      return new Response('WebSocket endpoint: /ws?room=ABCDEF', { status: 426 });

    const code = (url.searchParams.get('room') || '').toUpperCase();
    if (!okCode(code))
      return new Response('Invalid room code', { status: 400 });

    return env.ARENA_ROOMS.getByName(code).fetch(request);
  }
};

export class ArenaRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.meta = null;
    this.ctx.blockConcurrencyWhile(async () => {
      this.meta = await ctx.storage.get('meta') || null;
    });
  }

  sessions() {
    return this.ctx.getWebSockets().map(ws => ({
      ws, info: ws.deserializeAttachment() || {}
    }));
  }

  currentHost() {
    return this.sessions().find(s => s.info.role === 'host');
  }

  guests() {
    return this.sessions().filter(s => s.info.role === 'guest');
  }

  publish(message, except = null) {
    for (const { ws } of this.sessions())
      if (ws !== except) send(ws, message);
  }

  roomMessage() {
    const h = this.currentHost();
    return {
      type: 'room',
      code: this.meta?.code || '',
      guest: this.guests().length > 0,
      hostCharacter: this.meta?.hostCharacter || 0,
      guestCharacter: this.guests()[0]?.info.character || 0,
      arena: this.meta?.arena || 0,
      mode: this.meta?.mode || 'duel',
      playing: !!this.meta?.playing,
      players: [
        ...(h ? [{
          slot: 1,
          name: 'P1',
          character: this.meta?.hostCharacter || 0,
          profileIcon: h.info.profileIcon ?? 0,
          heroes: [this.meta?.hostCharacter || 0]
        }] : []),
        ...this.guests().map(g => ({
          slot: g.info.slot,
          name: 'P' + g.info.slot,
          character: g.info.character || 0,
          profileIcon: g.info.profileIcon ?? 0,
          heroes: g.info.heroes || [g.info.character || 0]
        }))
      ]
    };
  }

  roomUpdate() {
    if (this.meta) this.publish(this.roomMessage());
  }

  async persist() {
    if (this.meta) await this.ctx.storage.put('meta', this.meta);
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      role: 'pending', slot: 0, character: 0, profileIcon: 0
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 180000) {
      ws.close(1009, 'Message too large');
      return;
    }

    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.type !== 'string') return;

    const info = ws.deserializeAttachment() || {};

    if (m.type === 'create' && info.role === 'pending') {
      if (this.currentHost()) {
        send(ws, { type: 'error', message: 'Этот код уже занят.' });
        ws.close(1008, 'Room occupied');
        return;
      }

      if (!okCode(String(m.code || ''))) {
        send(ws, { type: 'error', message: 'Неверный код комнаты' });
        ws.close(1008, 'Invalid code');
        return;
      }

      this.meta = {
        code: m.code,
        hostCharacter: safeHero(m.character),
        arena: 0,
        mode: 'duel',
        playing: false
      };

      ws.serializeAttachment({
        role: 'host',
        slot: 1,
        character: this.meta.hostCharacter,
        profileIcon: 0
      });

      await this.persist();
      send(ws, { type: 'created', code: m.code, slot: 1 });
      this.roomUpdate();
      return;
    }

    if (m.type === 'join' && info.role === 'pending') {
      if (!this.currentHost() || !this.meta) {
        send(ws, {
          type: 'error',
          message: 'Комната не найдена. Создатель должен быть онлайн.'
        });
        ws.close(1008, 'Room missing');
        return;
      }

      if (this.meta.playing) {
        send(ws, { type: 'error', message: 'Матч уже начался.' });
        ws.close(1008, 'Playing');
        return;
      }

      if (this.guests().length >= 9) {
        send(ws, { type: 'error', message: 'Комната заполнена.' });
        ws.close(1008, 'Full');
        return;
      }

      const slots = new Set(this.guests().map(s => s.info.slot));
      let slot = 2;
      while (slots.has(slot)) slot++;

      const hero = safeHero(m.character);
      ws.serializeAttachment({
        role: 'guest',
        slot,
        character: hero,
        heroes: [hero],
        profileIcon: 0
      });

      send(ws, { type: 'joined', code: this.meta.code, slot });
      this.roomUpdate();
      return;
    }

    if (info.role === 'pending' || !this.meta) return;

    const host = info.role === 'host';

    if (m.type === 'profileIcon') {
      const profileIcon = safeHero(m.icon);
      if (info.profileIcon !== profileIcon) {
        ws.serializeAttachment({ ...info, profileIcon });
        this.roomUpdate();
      }
      return;
    }

    if (m.type === 'pick' && !this.meta.playing) {
      const hero = safeHero(m.character);
      if (host) {
        this.meta.hostCharacter = hero;
        await this.persist();
      } else {
        ws.serializeAttachment({
          ...info, character: hero, heroes: [hero]
        });
      }
      this.roomUpdate();
      return;
    }

    if (m.type === 'teamSelection' && !host && !this.meta.playing) {
      const heroes = Array.isArray(m.heroes)
        ? [...new Set(m.heroes.filter(
            n => Number.isInteger(n) && n >= 0 && n < 120
          ))].slice(0, 10)
        : [];

      ws.serializeAttachment({
        ...info,
        heroes,
        character: heroes[0] ?? info.character
      });
      this.roomUpdate();
      return;
    }

    if (host && m.type === 'mode' && !this.meta.playing) {
      this.meta.mode = String(m.mode || 'duel').slice(0, 40);
      await this.persist();
      this.roomUpdate();
      return;
    }

    if (host && m.type === 'arena' && !this.meta.playing) {
      this.meta.arena = Math.max(
        0, Math.min(99, Number(m.arena) || 0)
      );
      await this.persist();
      this.roomUpdate();
      return;
    }

    if (host && m.type === 'start' &&
        !this.meta.playing && this.guests().length) {
      this.meta.playing = true;
      await this.persist();

      this.publish({
        type: 'start',
        hostCharacter: this.meta.hostCharacter,
        guestCharacter: this.guests()[0]?.info.character || 0,
        arena: this.meta.arena,
        mode: this.meta.mode
      });

      this.roomUpdate();
      return;
    }

    if (host && m.type === 'reset') {
      this.meta.playing = false;
      await this.persist();
      this.roomUpdate();
      return;
    }

    if (!this.meta.playing) return;

    if (host && [
      'frame', 'status', 'result', 'roosterCast', 'roosterCancel'
    ].includes(m.type)) {
      for (const { ws: peer } of this.guests()) send(peer, m);
    } else if (!host && [
      'input', 'roosterCast', 'roosterCancel'
    ].includes(m.type)) {
      const h = this.currentHost();
      if (h) send(h.ws, { ...m, slot: info.slot });
    }
  }

  async webSocketClose(ws, code, reason) {
    await this.drop(ws);
  }

  async webSocketError(ws) {
    await this.drop(ws);
  }

  async drop(ws) {
    const info = ws.deserializeAttachment() || {};
    try { ws.close(1000, 'Disconnected'); } catch {}

    if (info.role === 'host') {
      this.publish({
        type: 'closed',
        reason: 'Хост вышел из комнаты.'
      }, ws);

      for (const { ws: other } of this.sessions()) {
        if (other !== ws) {
          try { other.close(1000, 'Host left'); } catch {}
        }
      }

      this.meta = null;
      await this.ctx.storage.delete('meta');
    } else if (info.role === 'guest') {
      this.publish({ type: 'guestLeft', slot: info.slot }, ws);
      this.roomUpdate();
    }
  }
}
