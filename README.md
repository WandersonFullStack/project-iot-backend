# IoT Gateway Backend

API FastAPI que integra PostgreSQL, MQTT, TCP e Modbus TCP. O ambiente
containerizado utiliza quatro processos:

- `backend`: API HTTP/WebSocket e gateways industriais;
- `postgres`: banco de dados interno;
- `mosquitto`: broker MQTT autenticado e criptografado na porta `8883`;
- proxy Traefik do EasyPanel: HTTPS e domínio público da API. 

## Segurança MQTT

O Compose não abre uma porta MQTT sem criptografia. O Mosquitto aceita apenas:

- MQTT sobre TLS em `8883`;
- usuário e senha;
- clientes que confiem na CA privada do projeto.

No primeiro start, o entrypoint do Mosquitto:

1. cria uma autoridade certificadora (CA) privada;
2. cria um certificado de servidor com os nomes `mosquitto` e
   `MQTT_CERT_HOST`;
3. cria o arquivo de senha;
4. salva certificados e credenciais em volumes persistentes.

O certificado do servidor é renovado quando faltam menos de 30 dias para
expirar. A CA e sua chave permanecem no volume e não entram no repositório.

> Trocar somente a porta para `8883` não oferece segurança. A proteção vem da
> validação TLS e da autenticação configuradas neste projeto.

## Variáveis de ambiente

Copie o exemplo apenas para desenvolvimento local:

```bash
cp .env.example .env
```

Troque todas as senhas e chaves antes do deploy. Gere o segredo JWT, por
exemplo, com:

```bash
openssl rand -hex 32
```

Variáveis obrigatórias:

- `POSTGRES_DB`: nome do banco;
- `POSTGRES_USER`: usuário do banco;
- `POSTGRES_PASSWORD`: senha do banco, usando caracteres URL-safe;
- `JWT_SECRET_KEY`: chave de assinatura dos tokens;
- `MQTT_USERNAME`: usuário do broker;
- `MQTT_PASSWORD`: senha do broker;
- `MQTT_CERT_HOST`: domínio público MQTT, por exemplo `mqtt.exemplo.com`.

O backend recebe automaticamente, pelo Compose:

```text
DATABASE_URL=postgresql+asyncpg://...@postgres:5432/...
MQTT_HOST=mosquitto
MQTT_PORT=8883
MQTT_TLS_ENABLED=true
MQTT_CA_CERT=/mosquitto-certs/ca.crt
```

## Execução local com Docker

Requisitos: Docker Engine e Docker Compose v2.

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f backend mosquitto
```

Serviços publicados:

- `8883/TCP`: MQTT sobre TLS; exige CA, usuário e senha;
- `9000/TCP`: gateway TCP dos dispositivos;
- `502/TCP`: Modbus TCP, mapeada para `1502` no container.

A API usa apenas a porta interna `8000`. Para desenvolvimento sem proxy, é
possível publicar temporariamente `8000:8000` no serviço `backend`.

Para encerrar sem apagar dados:

```bash
docker compose down
```

Não use `docker compose down -v` em produção: essa opção apaga banco,
certificados e dados persistentes.

## Conectar um cliente MQTT

Exporte somente o certificado público da CA:

```bash
docker compose cp mosquitto:/mosquitto/certs/ca.crt ./mqtt-ca.crt
```

Teste a publicação:

```bash
mosquitto_pub \
  -h mqtt.exemplo.com \
  -p 8883 \
  --cafile ./mqtt-ca.crt \
  -u backend \
  -P 'SENHA_MQTT' \
  -t application/devices/test/temperature \
  -m '{"value": 25}' \
  -q 1
```

Distribua `mqtt-ca.crt` aos dispositivos que acessarão o broker. Nunca
distribua `ca.key` ou `server.key`.

## Deploy no EasyPanel

### 1. DNS e firewall

Crie registros DNS apontando para o servidor:

```text
api.exemplo.com  -> IP do EasyPanel
mqtt.exemplo.com -> IP do EasyPanel
```

Mantenha `80` e `443` disponíveis para o EasyPanel e libere somente as portas
industriais realmente utilizadas:

```text
8883/TCP  MQTT TLS
9000/TCP  gateway TCP, se necessário externamente
502/TCP   Modbus TCP, se necessário externamente
```

O PostgreSQL não possui porta pública.

### 2. Serviço Compose

1. Crie um projeto no EasyPanel.
2. Adicione um serviço do tipo **Compose**.
3. Selecione o repositório do backend.
4. Informe `compose.yml` como arquivo Compose.
5. Cadastre o conteúdo de `.env.example` na seção **Environment**, substituindo
   todos os valores.
6. Faça o deploy.

Os volumes nomeados preservam PostgreSQL, dados MQTT e certificados entre
deploys.

### 3. Proxy reverso da API

Na seção **Domains** do Compose:

1. adicione `api.exemplo.com`;
2. selecione o serviço `backend`;
3. use protocolo HTTP;
4. use a porta interna `8000`;
5. habilite HTTPS e Let's Encrypt.

O Traefik do EasyPanel encaminha HTTP e WebSocket, incluindo `/api/v1/ws`.
Não adicione Nginx ou outro proxy para a API.

### 4. MQTT não passa pelo proxy HTTP

`8883` é publicado diretamente pelo Docker porque MQTT não é HTTP. O
certificado TLS do broker é gerenciado pelo container Mosquitto, não pelo
certificado HTTPS do Traefik.

Depois do primeiro deploy, abra o terminal do serviço Mosquitto no EasyPanel e
copie o conteúdo de:

```text
/mosquitto/certs/ca.crt
```

Salve-o como `mqtt-ca.crt` e instale-o nos clientes MQTT.

## Certificados e mudança de domínio

Quando `MQTT_CERT_HOST` muda, o Mosquitto emite automaticamente um novo
certificado de servidor assinado pela mesma CA. Reinicie/reimplante o serviço
para aplicar a mudança.

O certificado sempre contém:

```text
DNS:mosquitto
DNS:<MQTT_CERT_HOST>
```

Assim, o backend valida a conexão pela rede interna e dispositivos externos
validam pelo domínio público.

## Diagnóstico

Verifique o estado:

```bash
docker compose ps
docker compose logs backend
docker compose logs mosquitto
```

Erros comuns:

- `certificate verify failed`: cliente sem `mqtt-ca.crt` ou hostname diferente
  de `MQTT_CERT_HOST`;
- `not authorised`: usuário ou senha MQTT incorretos;
- `connection refused`: porta `8883` bloqueada no firewall;
- backend aguardando Mosquitto: confira o healthcheck e as credenciais;
- erro no PostgreSQL: confira senha URL-safe e o volume existente.
