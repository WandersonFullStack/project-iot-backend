from __future__ import annotations
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
        self._initialize_schema()

    @contextmanager
    def _connection(self):
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

    def _initialize_schema(self):
        """
        Cria as tabelas caso ainda não existam.
        Executado uma única vez na inicialização da controller.
        """

        with self._lock, self._connection() as conn:
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

# -> Escritas

    def insert_messages(
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

        with self._lock, self._connection() as conn:
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
        
    def register_publication(self, topic: str, payload: str, qos: int, mid: int) -> int:
        """
        Grava uma mensagem publicada por este cliente antes da confirmação
        do broker.
        """

        with self._lock, self._connection() as conn:
            cur = conn.execute(
                """
                INSERT INTO publications (topic, payload, qos, mid)
                VALUES (?, ?, ?, ?)
                """,
                (topic, payload, qos, mid),
            )

            return cur.lastrowid
        
    def confirm_publication(self, mid: int):
        """
        Atualiza o timestamp de confirmação quando o broker envia o PUBACK/PUBCOMP.
        Chamado pelo on_publish.
        """

        with self._lock, self._connection() as conn:
            conn.execute(
                "UPDATE publications SET confirmed_in = ? WHERE mid = ?",
                (datetime.now().isoformat(), mid),
            )

# -> Leitura

    def list_messages(
            self, 
            topic: str | None = None,
            limit: int = 10,
            offset: int = 0
        ) -> list[sqlite3.Row]:
        """
        Consulta as mensagens paginadas com filtro opcional por tópico.
        """

        with self._connection() as conn:
            if topic:
                return conn.execute(
                    """
                    SELECT * FROM received_messages
                    WHERE topic LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?
                    """,
                    (topic, limit, offset,),
                ).fetchall()
            
            return conn.execute(
                "SELECT * FROM received_messages ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset,),
            ).fetchall()
        
    def search_message(self, message_id: int) -> sqlite3.Row | None:
        with self._connection() as conn:
            return conn.execute(
                "SELECT * FROM received_messages WHERE id=?",
                (message_id,)
            ).fetchone()
        
    def delete_message(self, message_id: int) -> bool:
        with self._lock, self._connection() as conn:
            cur = conn.execute(
                "DELETE FROM received_messages WHERE id=?",
                (message_id,)
            )
            return cur.rowcount > 0
        
    def list_publications(self, limit: int = 20, offset: int = 0) -> list[sqlite3.Row]:
        with self._connection() as conn:
            return conn.execute(
                "SELECT * FROM publications ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset,)
            ).fetchall()
        
    def distinct_topics(self) -> list[str]:
        """Retorna todos os tópicos únicos já recebidos - útil para discovery."""
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT topic FROM received_messages ORDER BY topic"
            ).fetchall()
            return [r["topic"] for r in rows]
        
    def tell_messages(self, topic: str | None = None) -> int:
        with self._connection() as conn:
            if topic:
                return conn.execute(
                    "SELECT COUNT(*) FROM received_messages WHERE topic LIKE ?",
                    (topic,)
                ).fetchall()[0]
            
            return conn.execute(
                "SELECT COUNT(*) FROM received_messages"
            ).fetchall()[0]
        
    def tell_publications(self) -> int:
        with self._connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM plublications"
            ).fetchall()[0]