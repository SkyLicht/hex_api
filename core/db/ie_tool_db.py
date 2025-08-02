from sqlalchemy import create_engine, event
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# Base class for defining ORM orm_models.
IEToolBase = declarative_base()

class IETOOLDBConnection:
    """
    Singleton class to manage database connection and session factories.
    Ensures only one database connection exists and provides thread-safe sessions.
    """

    _instance = None  # Class-level attribute to hold the singleton instance

    # For testing, consider using an in-memory SQLite database: "sqlite:///:memory:"
    # e.g., DBConnection.DATABASE_URL = "sqlite:///:memory:"
    DATABASE_URL = "sqlite:///C:/data/hex_database/hex_db.db"

    def __new__(cls):
        """
        Override __new__ to implement the Singleton pattern. If the class has
        no existing instance, create one; otherwise, return the existing instance.
        """
        if cls._instance is None:
            cls._instance = super(IETOOLDBConnection, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """
        Initialize the database engine, create tables if needed,
        and set up session factories.
        """
        try:
            # Using connect_args for SQLite multi-threading support
            self.engine = create_engine(
                self.DATABASE_URL,
                connect_args={"check_same_thread": False},
                echo=False,
            )
        except SQLAlchemyError as e:
            # You could log the error or handle it in another way, as needed
            print(f"[Error] Failed to create database engine: {e}")
            raise  # Re-raise or handle gracefully depending on your application’s needs

        try:
            # Ensure all tables from Base are created.
            IEToolBase.metadata.create_all(bind=self.engine)
        except SQLAlchemyError as e:
            print(f"[Error] Failed to create tables: {e}")
            raise

        # Listen for the "connect" event, so that every new connection
        # automatically executes PRAGMA foreign_keys = ON.
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON;")
            cursor.close()

        IEToolBase.metadata.create_all(bind=self.engine)

        # Create a session factory
        self.SessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False
        )

        # Create a thread-safe scoped session
        self.ScopedSession = scoped_session(self.SessionFactory)

    def create_table(self, model):
        """
        Create a specific table in the database.

        Args:
            model (Base): SQLAlchemy ORM model class that inherits from Base.

        Example:
            class ExampleModel(Base):
                __tablename__ = 'example'
                id = Column(Integer, primary_key=True)
                name = Column(String)

            DBConnection().create_table(ExampleModel)
        """
        try:
            model.metadata.create_all(self.engine)
            print(f"Table for model '{model.__tablename__}' created successfully.")
        except SQLAlchemyError as e:
            print(f"Error creating table for model '{model.__tablename__}': {e}")

    def get_session(self):
        """
        Create and return a new, regular (non-scoped) database session.

        **Use Case**:
        - A single-threaded environment or scenarios where you manually
          manage session lifecycle.

        Example:
            with DBConnection().get_session() as session:
                session.add(new_object)
                session.commit()
        """
        return self.SessionFactory()

    def get_scoped_session(self):
        """
        Return a thread-safe scoped session.

        **Use Case**:
        - In multi-threaded applications or frameworks (e.g., Flask, FastAPI),
          where each thread or requests needs an isolated session.

        Example:
            session = DBConnection().get_scoped_session()
            item = session.query(Model).filter_by(id=1).first()
        """
        return self.ScopedSession

    def remove_scoped_session(self):
        """
        Remove the current thread's scoped session.

        This is typically called at the end of a web requests (or any
        time you are done with a thread) to ensure the session is
        properly closed and does not leak connections.
        """
        self.ScopedSession.remove()

    def close_engine(self):
        """
        Dispose of the underlying engine and release its resources.

        Useful when shutting down an application, or in unit tests
        when you want to ensure connections are cleaned up.
        """
        self.engine.dispose()
        print("Database engine disposed.")


