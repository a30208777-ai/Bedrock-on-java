"""
Relay de 3 portas para Minecraft (só biblioteca padrão do Python 3.9+).

  Porta 1 - HTTP  ($PORT no Render): só para o Render ver que está online
  Porta 2 - UDP   (19132): Bedrock  -> BEDROCK_HOST:BEDROCK_PORT
  Porta 3 - TCP   (25565): Java     -> JAVA_HOST:JAVA_PORT

IMPORTANTE: no Render só a porta HTTP é acessível pela internet.
As portas UDP e TCP funcionam em hosts que liberam portas brutas
(Fly.io, VPS, túnel Playit.gg, rede local).

Variáveis de ambiente:
  PORT            porta HTTP (o Render define sozinho)      padrão 8080
  UDP_PORT        porta UDP de entrada                       padrão 19132
  TCP_PORT        porta TCP de entrada                       padrão 25565
  BEDROCK_HOST    destino do UDP (Geyser Standalone/Bedrock) padrão 127.0.0.1
  BEDROCK_PORT                                               padrão 19133
  JAVA_HOST       destino do TCP (servidor Java)             padrão 127.0.0.1
  JAVA_PORT                                                  padrão 25566
"""
import asyncio
import json
import os
import time

HTTP_PORT = int(os.getenv("PORT", "8080"))
UDP_PORT = int(os.getenv("UDP_PORT", "19132"))
TCP_PORT = int(os.getenv("TCP_PORT", "25565"))
BEDROCK = (os.getenv("BEDROCK_HOST", "127.0.0.1"), int(os.getenv("BEDROCK_PORT", "19133")))
JAVA = (os.getenv("JAVA_HOST", "127.0.0.1"), int(os.getenv("JAVA_PORT", "25566")))
UDP_IDLE = 60  # segundos sem pacote para fechar a sessão UDP

START = time.time()
stats = {"tcp_conexoes": 0, "tcp_ativas": 0, "udp_sessoes": 0, "udp_ativas": 0}


# ---------------- Porta 1: HTTP (health check) ----------------
async def http_handler(reader, writer):
    try:
        await asyncio.wait_for(reader.readline(), timeout=5)
        corpo = json.dumps({
            "status": "online",
            "uptime_s": int(time.time() - START),
            "portas": {"http": HTTP_PORT, "udp": UDP_PORT, "tcp": TCP_PORT},
            **stats,
        }).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(corpo)).encode() + b"\r\nConnection: close\r\n\r\n" + corpo
        )
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


# ---------------- Porta 3: TCP (Java) ----------------
async def pipe(src, dst):
    try:
        while True:
            dados = await src.read(65536)
            if not dados:
                break
            dst.write(dados)
            await dst.drain()
    except Exception:
        pass
    finally:
        try:
            dst.close()
        except Exception:
            pass


async def tcp_handler(c_reader, c_writer):
    stats["tcp_conexoes"] += 1
    stats["tcp_ativas"] += 1
    try:
        s_reader, s_writer = await asyncio.wait_for(asyncio.open_connection(*JAVA), timeout=10)
        await asyncio.gather(pipe(c_reader, s_writer), pipe(s_reader, c_writer))
    except Exception as e:
        print(f"[TCP] falha ao conectar em {JAVA}: {e}")
    finally:
        stats["tcp_ativas"] -= 1
        c_writer.close()


# ---------------- Porta 2: UDP (Bedrock) ----------------
class UdpSessao(asyncio.DatagramProtocol):
    """Um socket UDP por jogador, ligado ao destino Bedrock."""

    def __init__(self, relay, cliente):
        self.relay = relay
        self.cliente = cliente
        self.transport = None
        self.pendentes = []
        self.ultimo = time.time()

    async def iniciar(self):
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(lambda: self, remote_addr=BEDROCK)

    def connection_made(self, transport):
        self.transport = transport
        for p in self.pendentes:
            transport.sendto(p)
        self.pendentes.clear()

    def enviar(self, dados):
        self.ultimo = time.time()
        if self.transport:
            self.transport.sendto(dados)
        else:
            self.pendentes.append(dados)

    def datagram_received(self, dados, addr):
        self.ultimo = time.time()
        self.relay.transport.sendto(dados, self.cliente)

    def fechar(self):
        if self.transport:
            self.transport.close()


class UdpRelay(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None
        self.sessoes = {}

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, dados, addr):
        s = self.sessoes.get(addr)
        if s is None:
            s = UdpSessao(self, addr)
            self.sessoes[addr] = s
            stats["udp_sessoes"] += 1
            stats["udp_ativas"] = len(self.sessoes)
            asyncio.ensure_future(s.iniciar())
        s.enviar(dados)

    async def limpar(self):
        while True:
            await asyncio.sleep(10)
            agora = time.time()
            for addr, s in list(self.sessoes.items()):
                if agora - s.ultimo > UDP_IDLE:
                    s.fechar()
                    del self.sessoes[addr]
            stats["udp_ativas"] = len(self.sessoes)


# ---------------- Main ----------------
async def main():
    loop = asyncio.get_running_loop()

    await asyncio.start_server(http_handler, "0.0.0.0", HTTP_PORT)
    print(f"[HTTP] porta {HTTP_PORT} (health check)")

    await asyncio.start_server(tcp_handler, "0.0.0.0", TCP_PORT)
    print(f"[TCP ] porta {TCP_PORT} -> {JAVA[0]}:{JAVA[1]} (Java)")

    udp = UdpRelay()
    await loop.create_datagram_endpoint(lambda: udp, local_addr=("0.0.0.0", UDP_PORT))
    print(f"[UDP ] porta {UDP_PORT} -> {BEDROCK[0]}:{BEDROCK[1]} (Bedrock)")
    asyncio.ensure_future(udp.limpar())

    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
