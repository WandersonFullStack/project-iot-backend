import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime

class DatabaseController:
    """
    Gerencia todas as operações de persistência no SQLite.
    Thread-safe: usa threading.Lock() para evitar escrita concorrente.
    """
    def __init__(self, caminho: str):
        self.caminho = caminho
        self._lock = threading.Lock()
        self._inicializar_schema()

    @contextmanager
    def _conexao(self):
        """
        Gerenciaador de contexto que abre e fecha a conexão de forma segura.
        Cada chamada cria uma nova conexão.
        """

        conn = sqlite3.connect(self.caminho, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL") # modo de alta concorrência
        conn.row_factory = sqlite3.Row  # resultado como dicionário

        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _inicializar_schema(self):
        """
        Cria as tabelas caso ainda não existam.
        Executado uma única vez na inicialização da controller.
        """

        with self._lock, self._conexao() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS received_messages (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic           TEXT    NOT NULL,
                    payload         TEXT,
                    qos             INTEGER DEFAULT 0,
                    retain          INTEGER DEFAULT 0,
                    content_type    TEXT,
                    user_props      TEXT,
                    received_in     TEXT    NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS publications (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic           TEXT    NOT NULL,
                    payload         TEXT,
                    qos             INTEGER DEFAULT 0,
                    mid             INTEGER,
                    confirmed_in    TEXT
                );
                
                CREATE INDEX IF NOT EXISTS index_msg_topic
                    ON received_messages(topic);
                
                CREATE INDEX IF NOT EXISTS index_pub_mid
                    ON publications(mid);
            """)

    def inserir_mensagens(
            self,
            topic: str,
            payload: str,
            qos: int,
            retain: bool,
            content_type: str | None = None,
            user_props: str | None = None,
    ) -> int:
        """
        Persiste uma mensagem MQTT recebida.
        Retorna int: id do registro inserido
        """

        with self._lock, self._conexao() as conn:
            cur = conn.execute(
                """
                INSERT INTO received_messages
                    (topic, payload, qos, retain, content_type, user_props, received_in)

                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    topic, 
                    payload, 
                    qos, 
                    int(retain), 
                    content_type, 
                    user_props, 
                    datetime.now().isoformat()
                ),
            )

            return cur.lastrowid
        
    def registrar_publicacao(self, topic: str, payload: str, qos: int, mid: int) -> int:
        """
        Grava uma mensagem publicada por este cliente antes da confirmação
        do broker.
        """

        with self._lock, self._conexao() as conn:
            cur = conn.execute(
                """
                INSERT INTO publications (topic, payload, qos, mid)
                VALUES (?, ?, ?, ?)
                """,
                (topic, payload, qos, mid),
            )

            return cur.lastrowid
        
    def confirmar_publicacao(self, mid: int):
        """
        Atualiza o timestamp de confirmação quando o broker envia o PUBACK/PUBCOMP.
        Chamado pelo on_publish.
        """

        with self._lock, self._conexao() as conn:
            conn.execute(
                "UPDATE publications SET confirmed_in = ? WHERE mid = ?",
                (datetime.now().isoformat(), mid),
            )

    def ultimas_mensagens(self, limit: int = 10) -> list[sqlite3.Row]:
        """
        Consulta as mensagens mais recentes para qualquer tópico.
        """

        with self._conexao() as conn:
            return conn.execute(
                """
                SELECT topic, payload, qos, retain, received_in
                FROM received_messages
                ORDER BY id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()