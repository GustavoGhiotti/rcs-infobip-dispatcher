# Robo Envio RCS - Infobip

> **Versão de portfólio.** Código real de produção, anonimizado: nomes de empresa, clientes, pessoas, hosts, e-mails e credenciais foram trocados por valores fictícios (`Acme`, `Banco Alfa`, `10.0.0.x`, `empresa.example`). Toda configuração sensível vem do `.env` (modelo em `.env.example`). O código não aponta para nenhum sistema real.

Robo apartado baseado no fluxo do `RoboEnvioSMS-Infobip`, mantendo as tratativas de fila, limites, reservas, status, spool e alertas, mas enviando mensagens RCS pela API da Infobip.

## Execucao

```bat
run_enviar.bat
run_status.bat
run_arquivar.bat
```

Ou diretamente:

```bash
python -m main enviar
python -m main status
python -m main relatorio
python -m main arquivar
```

## Configuracao

Crie o `.env` a partir do `.env.example` e configure:

- `INFOBIP_BASE_URL`
- `INFOBIP_API_KEY`
- `INFOBIP_RCS_FROM`
- dados do SQL Server
- dados de e-mail

## Infobip RCS

O envio usa:

- `POST /rcs/2/messages`
- `GET /rcs/2/reports`
- `GET /rcs/2/logs`

O payload enviado e do tipo cartao avancado (`CARD`).

- `MENSAGEM`: texto/descricao exibido no card.
- `VARIAVEIS_JSON`: configuracao obrigatoria do card, com imagem e botoes URL. `titulo` e opcional.
- Padrao automatico do card: orientacao vertical e imagem media.
- Em cada botao, `postbackData` usa o mesmo valor de `texto`.

```json
{
  "imagem": "https://site.com/imagem.png",
  "botoes": [
    {
      "texto": "Abrir boleto",
      "url": "https://site.com/boleto"
    },
    {
      "texto": "Comprovante",
      "url": "https://site.com/comprovante"
    }
  ]
}
```

Todos os botoes devem ser URL (`OPEN_URL`). Se o JSON estiver invalido ou faltar campo obrigatorio, o robo nao envia e
marca o registro como `MSG_FORA_PADRAO`.

## Banco

Execute a migration:

```sql
scripts/001_create_rcs_tables.sql
scripts/002_create_rcs_history.sql
```

Ela cria as estruturas RCS vazias, usando as tabelas SMS apenas como modelo:

- `infobip.TB_ENVIO_RCS_MENSAGEM`
- `infobip.TB_ENVIO_RCS_MENSAGEM_HIST`
- `infobip.VW_ENVIO_RCS_MENSAGEM_LEITURA`
- `infobip.TB_CEDENTE_RCS_LIMITE_SEMANAL`
- `infobip.TB_CEDENTE_RCS_LIMITE_MENSAL`
- `infobip.TB_RCS_LIMITE_TOTAL_SEMANAL`
- `infobip.TB_RCS_LIMITE_TOTAL_MENSAL`

O robo RCS aponta para essas estruturas e nao consome a fila SMS.

Para arquivar a fila RCS antiga e manter a tabela principal leve:

```sql
EXEC infobip.SP_ARQUIVAR_TB_ENVIO_RCS_MENSAGEM;
```

O `status` tambem executa esse arquivamento ao final do processamento. Se quiser rodar manualmente:

```bat
run_arquivar.bat
```

## Inserir caso para envio

`DT_INCLUSAO` e preenchido automaticamente com `GETDATE()`.

```sql
INSERT INTO infobip.TB_ENVIO_RCS_MENSAGEM (
    ID_CEDENTE,
    CEDENTE,
    CANAL,
    NM_USUARIO_ENVIO,
    NUM_CLIENTE,
    MENSAGEM,
    VARIAVEIS_JSON,
    STATUS
)
VALUES (
    1,
    'NOME DO CEDENTE',
    'RCS',
    'USUARIO_ROBO',
    '11999999999',
    'Ola, seu boleto ja esta disponivel para pagamento.',
    '{
      "imagem": "https://site.com/imagem.png",
      "botoes": [
        {
          "texto": "Abrir boleto",
          "url": "https://site.com/boleto"
        },
        {
          "texto": "Comprovante",
          "url": "https://site.com/comprovante"
        }
      ]
    }',
    'PENDENTE'
);
```
