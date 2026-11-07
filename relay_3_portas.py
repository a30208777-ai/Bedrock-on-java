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
  TOKEN           senha dos mods (rotas /pos e /players)    obrigatório em produção

Rotas HTTP:
  GET  /          status
  POST /pos       mods enviam {"origem":"java"|"bedrock","jogadores":[...]}
  GET  /players   devolve {"java":[...],"bedrock":[...]}
  POST /blocos    Java envia {"mudancas":[{"x","y","z","mundo","bloco":"minecraft:oak_stairs[facing=north]"}]}
  GET  /blocos?apos=N   Bedrock busca mudanças já traduzidas com seq > N
  GET  /blocos/aproximados   blocos que perderam propriedades na tradução
  POST /chunk     mods enviam um chunk varrido (quem não tem dono vira dono)
  GET  /chunk/dono?mundo=&cx=&cz=   quem é dono do chunk (null = ninguém)
  GET  /chunks?lado=java|bedrock&apos=N   chunks já traduzidos para aquele lado
  POST /floodgate/identidade   {xuid, gamertag} -> {uuid_java, nome_java} (regras do Floodgate)
  POST /link/codigo            {xuid, gamertag} -> {codigo} de 4 dígitos, vale 300 s
  POST /link/confirmar         {codigo, java_uuid, java_name} -> vincula as contas
  GET  /link?xuid= ou ?java_uuid=   consulta o vínculo
  GET/POST /config   {"rota": "ambos"|"bedrock->java"|"java->bedrock"|"off"}
  POST /comando      {origem, jogador, comando} -> entrega ao outro lado
  GET  /comandos?lado=java|bedrock&apos=N
  SUPABASE_URL / SUPABASE_KEY (opcional)  guardam os donos dos chunks (tabela chunks_donos)
"""
import asyncio
import json
import os
import random
import re
import time
import uuid
from collections import deque
from urllib.parse import parse_qs
import urllib.request

HTTP_PORT = int(os.getenv("PORT", "8080"))
UDP_PORT = int(os.getenv("UDP_PORT", "19132"))
TCP_PORT = int(os.getenv("TCP_PORT", "25565"))
BEDROCK = (os.getenv("BEDROCK_HOST", "127.0.0.1"), int(os.getenv("BEDROCK_PORT", "19133")))
JAVA = (os.getenv("JAVA_HOST", "127.0.0.1"), int(os.getenv("JAVA_PORT", "25566")))
UDP_IDLE = 60  # segundos sem pacote para fechar a sessão UDP

START = time.time()
stats = {"tcp_conexoes": 0, "tcp_ativas": 0, "udp_sessoes": 0, "udp_ativas": 0}


# ---------------- Tabela de tradução de blocos Java -> Bedrock ----------------
# ATENÇÃO: lista parcial, baseada nas diferenças conhecidas. O Bedrock muda nomes
# entre versões: se algo sair errado, corrija SEM mexer no código criando o arquivo
# mapeamento_extra.json ao lado deste script, no formato:
#   {"renomear": {"nome_java": "nome_bedrock"}}
# Blocos que não estão aqui passam com o MESMO nome (a grande maioria é igual).

RENOMEAR = {
    "dirt_path": "grass_path",
    "terracotta": "hardened_clay",
    "light_gray_glazed_terracotta": "silver_glazed_terracotta",
    "nether_portal": "portal",
    "shulker_box": "undyed_shulker_box",
    "oak_sign": "standing_sign",
    "spruce_sign": "spruce_standing_sign",
    "birch_sign": "birch_standing_sign",
    "jungle_sign": "jungle_standing_sign",
    "acacia_sign": "acacia_standing_sign",
    "dark_oak_sign": "darkoak_standing_sign",
    "cave_air": "air",
    "void_air": "air",
}

# No Java isso é uma propriedade; no Bedrock (antigo) é outro bloco.
# (bloco_java, propriedade, valor) -> bloco_bedrock
ESTADO_VIRA_BLOCO = {
    ("furnace", "lit", "true"): "lit_furnace",
    ("blast_furnace", "lit", "true"): "lit_blast_furnace",
    ("smoker", "lit", "true"): "lit_smoker",
    ("redstone_ore", "lit", "true"): "lit_redstone_ore",
    ("deepslate_redstone_ore", "lit", "true"): "lit_deepslate_redstone_ore",
    ("redstone_lamp", "lit", "true"): "lit_redstone_lamp",
    ("redstone_torch", "lit", "false"): "unlit_redstone_torch",
    ("repeater", "powered", "true"): "powered_repeater",
    ("repeater", "powered", "false"): "unpowered_repeater",
    ("comparator", "powered", "true"): "powered_comparator",
    ("comparator", "powered", "false"): "unpowered_comparator",
}

DIRECAO_ESCADA = {"east": 0, "west": 1, "south": 2, "north": 3}

try:
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapeamento_extra.json"), encoding="utf-8") as f:
        RENOMEAR.update(json.load(f).get("renomear", {}))
        print("[BLOCOS] mapeamento_extra.json carregado")
except FileNotFoundError:
    pass
except Exception as e:
    print(f"[BLOCOS] erro no mapeamento_extra.json: {e}")

_RE_BLOCO = re.compile(r"^(?:minecraft:)?([a-z0-9_]+)(?:\[(.*)\])?$")


def traduzir_bloco(texto):
    """'minecraft:oak_stairs[facing=north,half=top]' -> {'id','estados','precisao'}
    precisao: exato (sem propriedades) | traduzido (propriedades convertidas) |
              aproximado (propriedades descartadas: orientação/estado pode sair diferente)"""
    m = _RE_BLOCO.match((texto or "").strip())
    if not m:
        return None
    nome, bruto = m.group(1), m.group(2)
    props = dict(p.split("=", 1) for p in bruto.split(",") if "=" in p) if bruto else {}
    estados, precisao = {}, "exato" if not props else "aproximado"

    for (b, prop, val), novo in ESTADO_VIRA_BLOCO.items():
        if nome == b and props.get(prop) == val:
            return {"id": "minecraft:" + novo, "estados": {}, "precisao": "aproximado" if len(props) > 1 else "traduzido"}

    if nome.endswith("_stairs") and "facing" in props:
        estados = {"weirdo_direction": DIRECAO_ESCADA.get(props["facing"], 0),
                   "upside_down_bit": props.get("half") == "top"}
        precisao = "traduzido"
    elif (nome.endswith("_log") or nome.endswith("_wood")) and "axis" in props:
        estados = {"pillar_axis": props["axis"]}
        precisao = "traduzido"
    elif nome.endswith("_slab") and "type" in props:
        if props["type"] == "double":
            nome = nome.replace("_slab", "_double_slab")
            precisao = "aproximado"
        else:
            estados = {"minecraft:vertical_half": props["type"]}
            precisao = "traduzido"

    nome = RENOMEAR.get(nome, nome)
    return {"id": "minecraft:" + nome, "estados": estados, "precisao": precisao}


def traduzir_mundo(nome):
    n = (nome or "").lower()
    if "nether" in n:
        return "minecraft:nether"
    if "end" in n:
        return "minecraft:the_end"
    return "minecraft:overworld"


fila_blocos = deque(maxlen=20000)
seq_blocos = 0
aproximados = {}  # blocos cujas propriedades foram descartadas -> quantas vezes


def registrar_blocos(mudancas):
    global seq_blocos
    aceitas = 0
    for c in mudancas[:500]:
        try:
            t = traduzir_bloco(c["bloco"])
            if not t:
                continue
            seq_blocos += 1
            aceitas += 1
            if t["precisao"] == "aproximado":
                aproximados[t["id"]] = aproximados.get(t["id"], 0) + 1
            fila_blocos.append({
                "seq": seq_blocos, "x": int(c["x"]), "y": int(c["y"]), "z": int(c["z"]),
                "dim": traduzir_mundo(c.get("mundo")),
                "id": t["id"], "estados": t["estados"], "precisao": t["precisao"],
            })
        except Exception:
            continue
    return {"ok": True, "aceitas": aceitas, "ultimo": seq_blocos}


# ---------------- Sincronização de chunks: quem anda gera, o outro lado copia ----------------
# Fluxo: cada mod olha os chunks perto dos jogadores e pergunta ao relay quem é o "dono".
# Sem dono -> o mod varre o chunk, envia, e vira dono. O relay traduz a paleta de blocos
# e entrega ao outro lado, que aplica. Chunk que já tem dono nunca é sobrescrito pelo outro lado.
# Formato do chunk: ordem y (de ymin a ymax), depois z, depois x. runs = [indice, qtd, indice, qtd...]
SB_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SB_KEY = os.getenv("SUPABASE_KEY", "")
EXTRA_INVERSO = {"flowing_water": "water", "flowing_lava": "lava"}
DIRECAO_ESCADA_INV = {0: "east", 1: "west", 2: "south", 3: "north"}

donos = {}
fila_chunks = {"java": deque(maxlen=300), "bedrock": deque(maxlen=300)}
seq_chunks = 0


def traduzir_bedrock(texto):
    """'minecraft:oak_stairs[weirdo_direction=3,upside_down_bit=true]' -> string de bloco do Java"""
    m = _RE_BLOCO.match((texto or "").strip())
    if not m:
        return "minecraft:air"
    nome, bruto = m.group(1), m.group(2)
    props = dict(p.split("=", 1) for p in bruto.split(",") if "=" in p) if bruto else {}
    nome = EXTRA_INVERSO.get(nome, nome)

    for (b, prop, val), novo in ESTADO_VIRA_BLOCO.items():
        if nome == novo:
            return f"minecraft:{b}[{prop}={val}]"

    jp = {}
    if nome.endswith("_stairs") and "weirdo_direction" in props:
        try:
            jp["facing"] = DIRECAO_ESCADA_INV.get(int(props["weirdo_direction"]), "north")
        except ValueError:
            jp["facing"] = "north"
        jp["half"] = "top" if props.get("upside_down_bit") == "true" else "bottom"
    elif nome.endswith(("_log", "_wood")) and "pillar_axis" in props:
        jp["axis"] = props["pillar_axis"]
    elif nome.endswith("_double_slab"):
        nome = nome.replace("_double_slab", "_slab")
        jp["type"] = "double"
    elif nome.endswith("_slab") and "minecraft:vertical_half" in props:
        jp["type"] = props["minecraft:vertical_half"]

    inverso = {v: k for k, v in RENOMEAR.items() if v != "air"}
    nome = inverso.get(nome, nome)
    return f"minecraft:{nome}" + ("[" + ",".join(f"{k}={v}" for k, v in jp.items()) + "]" if jp else "")


def _sb(metodo, caminho, corpo=None, extra=None):
    h = {"apikey": SB_KEY, "Authorization": "Bearer " + SB_KEY, "Content-Type": "application/json"}
    h.update(extra or {})
    req = urllib.request.Request(SB_URL + caminho, method=metodo, headers=h,
                                 data=json.dumps(corpo).encode() if corpo is not None else None)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"null")


def carregar_donos():
    """Lê do Supabase quem é dono de cada chunk (para não perder isso quando o Render reiniciar)."""
    if not (SB_URL and SB_KEY):
        print("[CHUNKS] sem SUPABASE_URL/SUPABASE_KEY: donos ficam só na memória (perdem-se ao reiniciar)")
        return
    try:
        pos = 0
        while True:
            linhas = _sb("GET", f"/rest/v1/chunks_donos?select=dim,cx,cz,dono&order=dim,cx,cz&limit=1000&offset={pos}")
            for r in linhas:
                donos[f"{r['dim']}:{r['cx']}:{r['cz']}"] = r["dono"]
            if len(linhas) < 1000:
                break
            pos += 1000
        print(f"[CHUNKS] {len(donos)} donos carregados do Supabase")
    except Exception as e:
        print(f"[CHUNKS] falha ao ler o Supabase: {e}")


def salvar_dono(dim, cx, cz, dono):
    try:
        _sb("POST", "/rest/v1/chunks_donos", [{"dim": dim, "cx": cx, "cz": cz, "dono": dono}],
            {"Prefer": "resolution=merge-duplicates,return=minimal"})
    except Exception as e:
        print(f"[CHUNKS] falha ao salvar dono: {e}")


def registrar_chunk(d):
    global seq_chunks
    origem = d.get("origem")
    if origem not in ("java", "bedrock"):
        raise ValueError("origem")
    if not rota_permite(origem):
        return {"ok": False, "erro": "rota bloqueia este lado", "rota": rota_config["rota"]}
    cx, cz = int(d["cx"]), int(d["cz"])
    mundo = traduzir_mundo(d.get("mundo"))
    chave = f"{mundo}:{cx}:{cz}"
    dono = donos.get(chave)
    if dono and dono != origem:
        return {"ok": False, "duplicado": True, "dono": dono}

    runs = [int(v) for v in d.get("runs", [])]
    if len(runs) % 2:
        raise ValueError("runs")
    brutos = d.get("palette", [])
    if origem == "java":
        destino, paleta = "bedrock", []
        for p in brutos:
            t = traduzir_bloco(p)
            paleta.append({"id": t["id"], "estados": t["estados"]} if t else {"id": "minecraft:air", "estados": {}})
    else:
        destino, paleta = "java", [traduzir_bedrock(p) for p in brutos]

    seq_chunks += 1
    fila_chunks[destino].append({
        "seq": seq_chunks, "mundo": mundo, "cx": cx, "cz": cz,
        "ymin": int(d["ymin"]), "ymax": int(d["ymax"]), "palette": paleta, "runs": runs,
    })
    if not dono:
        donos[chave] = origem
        if SB_URL and SB_KEY:
            asyncio.ensure_future(asyncio.to_thread(salvar_dono, mundo, cx, cz, origem))
    return {"ok": True, "destino": destino, "seq": seq_chunks}


# ---------------- Rota de sincronização e comandos entre os lados ----------------
# Rotas: ambos | bedrock->java (só Bedrock envia chunks) | java->bedrock (só Java envia) | off
ROTAS = {"ambos", "bedrock->java", "java->bedrock", "off"}
rota_config = {"rota": "ambos"}
fila_comandos = {"java": deque(maxlen=500), "bedrock": deque(maxlen=500)}
seq_comandos = 0


def rota_permite(origem):
    r = rota_config["rota"]
    return r == "ambos" or (r == "bedrock->java" and origem == "bedrock") or (r == "java->bedrock" and origem == "java")


def enfileirar_comando(origem, jogador, comando):
    global seq_comandos
    comando = (comando or "").strip().lstrip("/")
    if not comando or len(comando) > 300:
        raise ValueError("comando invalido")
    destino = "bedrock" if origem == "java" else "java"
    seq_comandos += 1
    fila_comandos[destino].append({"seq": seq_comandos, "de": origem, "jogador": jogador, "comando": comando})
    return seq_comandos


# ---------------- Floodgate: identidade e vínculo de contas Bedrock <-> Java ----------------
# Regras copiadas do config.yml do Floodgate:
#   username-prefix trocado para "[BEDROCK]" -> nome Java começa com [BEDROCK] (limite de 16 caracteres)
#   replace-spaces true   -> espaços viram "_"
#   player-link: código de 4 dígitos, expira em 300 s (link-code-timeout)
# UUID do jogador Bedrock = UUID(0, XUID), igual ao Floodgate.
FG_PREFIXO = "[BEDROCK]"
FG_TIMEOUT_CODIGO = 300
FG_NOME_MAX = 16  # limite de nome do Java

vinculos = {"por_xuid": {}, "por_java": {}}   # xuid -> java_uuid  e  java_uuid -> xuid
codigos_ativos = {}                             # codigo -> {xuid, gamertag, expira}


def fg_uuid_java(xuid):
    """UUID que o Java enxerga para um jogador Bedrock (Floodgate: new UUID(0, xuid))."""
    return str(uuid.UUID(int=int(xuid)))


def fg_nome_java(gamertag):
    nome = FG_PREFIXO + (gamertag or "").replace(" ", "_")
    return nome[:FG_NOME_MAX]


def fg_limpar_codigos():
    agora = time.time()
    for c in [c for c, v in codigos_ativos.items() if v["expira"] < agora]:
        del codigos_ativos[c]


def fg_criar_codigo(xuid, gamertag):
    fg_limpar_codigos()
    if str(xuid) in vinculos["por_xuid"]:
        return None, "conta ja vinculada"
    for _ in range(1000):
        c = f"{random.randint(0, 9999):04d}"
        if c not in codigos_ativos:
            codigos_ativos[c] = {"xuid": str(xuid), "gamertag": gamertag, "expira": time.time() + FG_TIMEOUT_CODIGO}
            return c, None
    return None, "sem codigos livres"


def fg_confirmar(codigo, java_uuid, java_name):
    fg_limpar_codigos()
    dados = codigos_ativos.pop(str(codigo), None)
    if not dados:
        return None, "codigo invalido ou expirado"
    vinculos["por_xuid"][dados["xuid"]] = java_uuid
    vinculos["por_java"][java_uuid] = dados["xuid"]
    if SB_URL and SB_KEY:
        asyncio.ensure_future(asyncio.to_thread(fg_salvar_vinculo, dados["xuid"], java_uuid, java_name))
    return {"xuid": dados["xuid"], "java_uuid": java_uuid, "java_name": java_name}, None


def fg_salvar_vinculo(xuid, java_uuid, java_name):
    try:
        _sb("POST", "/rest/v1/vinculos", [{"xuid": xuid, "java_uuid": java_uuid, "java_nome": java_name}],
            {"Prefer": "resolution=merge-duplicates,return=minimal"})
    except Exception as e:
        print(f"[LINK] falha ao salvar vinculo: {e}")


# ---------------- Porta 1: HTTP (health check + posições) ----------------
TOKEN = os.getenv("TOKEN", "")
jogadores = {"java": {"t": 0, "lista": []}, "bedrock": {"t": 0, "lista": []}}
VALIDADE = 30  # segundos até uma posição ser considerada velha


async def http_handler(reader, writer):
    try:
        linha = await asyncio.wait_for(reader.readline(), timeout=5)
        partes = linha.decode(errors="ignore").split()
        metodo = partes[0] if partes else "GET"
        alvo = partes[1] if len(partes) > 1 else "/"
        caminho, _, query = alvo.partition("?")
        headers = {}
        while True:
            l = await asyncio.wait_for(reader.readline(), timeout=5)
            if l in (b"\r\n", b"\n", b""):
                break
            k, _, v = l.decode(errors="ignore").partition(":")
            headers[k.strip().lower()] = v.strip()
        n = min(int(headers.get("content-length", "0") or 0), 4000000)
        corpo = await asyncio.wait_for(reader.readexactly(n), timeout=30) if n else b""

        status, resp = "200 OK", None
        if caminho.startswith(("/pos", "/players", "/blocos", "/chunk", "/link", "/floodgate", "/config", "/comando")) and TOKEN and headers.get("x-token") != TOKEN:
            status, resp = "401 Unauthorized", {"erro": "token invalido"}
        elif caminho == "/pos" and metodo == "POST":
            try:
                dados = json.loads(corpo or b"{}")
                origem = dados.get("origem")
                if origem in jogadores:
                    jogadores[origem] = {"t": time.time(), "lista": dados.get("jogadores", [])[:200]}
                    resp = {"ok": True}
                else:
                    status, resp = "400 Bad Request", {"erro": "origem deve ser java ou bedrock"}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "json invalido"}
        elif caminho == "/blocos" and metodo == "POST":
            try:
                resp = registrar_blocos(json.loads(corpo or b"{}").get("mudancas", []))
            except Exception:
                status, resp = "400 Bad Request", {"erro": "json invalido"}
        elif caminho == "/blocos/aproximados":
            resp = aproximados
        elif caminho == "/blocos":
            try:
                apos = int(parse_qs(query).get("apos", ["0"])[0])
            except ValueError:
                apos = 0
            resp = {"mudancas": [m for m in fila_blocos if m["seq"] > apos][:500], "ultimo": seq_blocos}
        elif caminho == "/chunk" and metodo == "POST":
            try:
                resp = registrar_chunk(json.loads(corpo or b"{}"))
            except Exception:
                status, resp = "400 Bad Request", {"erro": "chunk invalido"}
        elif caminho == "/chunk/dono":
            q = parse_qs(query)
            try:
                chave = f"{traduzir_mundo(q.get('mundo', [''])[0])}:{int(q['cx'][0])}:{int(q['cz'][0])}"
                resp = {"dono": donos.get(chave)}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "parametros invalidos"}
        elif caminho == "/chunks":
            q = parse_qs(query)
            lado = q.get("lado", [""])[0]
            try:
                apos = int(q.get("apos", ["0"])[0])
            except ValueError:
                apos = 0
            if lado in fila_chunks:
                resp = {"chunks": [c for c in fila_chunks[lado] if c["seq"] > apos][:2]}
            else:
                status, resp = "400 Bad Request", {"erro": "lado deve ser java ou bedrock"}
        elif caminho == "/floodgate/identidade" and metodo == "POST":
            try:
                d = json.loads(corpo or b"{}")
                resp = {"uuid_java": fg_uuid_java(d["xuid"]), "nome_java": fg_nome_java(d.get("gamertag", ""))}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "envie xuid e gamertag"}
        elif caminho == "/link/codigo" and metodo == "POST":
            try:
                d = json.loads(corpo or b"{}")
                codigo, erro = fg_criar_codigo(d["xuid"], d.get("gamertag", ""))
                if erro:
                    status, resp = "409 Conflict", {"erro": erro}
                else:
                    resp = {"codigo": codigo, "expira_em_s": FG_TIMEOUT_CODIGO}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "envie xuid e gamertag"}
        elif caminho == "/link/confirmar" and metodo == "POST":
            try:
                d = json.loads(corpo or b"{}")
                resp, erro = fg_confirmar(d["codigo"], d["java_uuid"], d.get("java_name", ""))
                if erro:
                    status, resp = "404 Not Found", {"erro": erro}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "envie codigo e java_uuid"}
        elif caminho == "/link":
            q = parse_qs(query)
            if "xuid" in q and q["xuid"][0] in vinculos["por_xuid"]:
                resp = {"xuid": q["xuid"][0], "java_uuid": vinculos["por_xuid"][q["xuid"][0]]}
            elif "java_uuid" in q and q["java_uuid"][0] in vinculos["por_java"]:
                resp = {"java_uuid": q["java_uuid"][0], "xuid": vinculos["por_java"][q["java_uuid"][0]]}
            else:
                status, resp = "404 Not Found", {"erro": "sem vinculo"}
        elif caminho == "/config" and metodo == "POST":
            try:
                r = json.loads(corpo or b"{}").get("rota")
                if r not in ROTAS:
                    status, resp = "400 Bad Request", {"erro": "rota deve ser: " + ", ".join(sorted(ROTAS))}
                else:
                    rota_config["rota"] = r
                    resp = {"rota": r}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "json invalido"}
        elif caminho == "/config":
            resp = {"rota": rota_config["rota"]}
        elif caminho == "/comando" and metodo == "POST":
            try:
                d = json.loads(corpo or b"{}")
                if d.get("origem") not in ("java", "bedrock"):
                    raise ValueError("origem")
                resp = {"ok": True, "seq": enfileirar_comando(d["origem"], str(d.get("jogador", "")), d.get("comando", ""))}
            except Exception:
                status, resp = "400 Bad Request", {"erro": "envie origem, jogador e comando"}
        elif caminho == "/comandos":
            q = parse_qs(query)
            lado = q.get("lado", [""])[0]
            try:
                apos = int(q.get("apos", ["0"])[0])
            except ValueError:
                apos = 0
            if lado in fila_comandos:
                resp = {"comandos": [c for c in fila_comandos[lado] if c["seq"] > apos][:20]}
            else:
                status, resp = "400 Bad Request", {"erro": "lado deve ser java ou bedrock"}
        elif caminho == "/players":
            agora = time.time()
            resp = {o: (d["lista"] if agora - d["t"] < VALIDADE else []) for o, d in jogadores.items()}
        else:
            resp = {
                "status": "online",
                "uptime_s": int(time.time() - START),
                "portas": {"http": HTTP_PORT, "udp": UDP_PORT, "tcp": TCP_PORT},
                **stats,
            }
        corpo_resp = json.dumps(resp).encode()
        writer.write(
            f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(corpo_resp)}\r\nConnection: close\r\n\r\n".encode() + corpo_resp
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
    await asyncio.to_thread(carregar_donos)

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
