package main

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"log"
)

func connect(logger *zap.Logger) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", "user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		logger.Error("failed to connect to db", zap.Error(err))
		return nil, err
	}
	logger.Info("connected to db")

	if err = db.Ping(); err != nil {
		logger.Error("failed to ping db", zap.Error(err))
		return nil, err
	}
	logger.Info("ping db - OK")
	return db, nil
}

func migrate(db *sqlx.DB, logger *zap.Logger) error {
	migrations := []string{
		`DROP TABLE IF EXISTS person;`,
		`CREATE TABLE IF NOT EXISTS person (
           id SERIAL PRIMARY KEY,
           balance BIGINT NOT NULL
         );`,
		`INSERT INTO person VALUES (1, 1000);`,
		`INSERT INTO person VALUES (2, 1000);`,
	}

	for _, m := range migrations {
		_, err := db.Exec(m)
		if err != nil {
			logger.Error("failed to execute migration", zap.Error(err), zap.String("migration", m))
			return err
		}
		logger.Info("migration executed", zap.String("migration", m))
	}
	logger.Info("all migrations executed")
	return nil
}

type transaction struct {
	db     *sqlx.DB
	tx     *sql.Tx
	logger *zap.Logger
}

func newTransaction(db *sqlx.DB, logger *zap.Logger) *transaction {
	return &transaction{db: db, logger: logger}
}

func (t *transaction) begin() error {
	tx1, err := t.db.Begin()
	if err != nil {
		t.logger.Error("failed to begin tx", zap.Error(err))
		return err
	}
	t.logger.Info("tx started")
	t.tx = tx1
	return nil
}

func (t *transaction) setLevel(level sql.IsolationLevel) error {
	var isolationLevelQuery = "SET TRANSACTION ISOLATION LEVEL " + level.String() + ";"
	if _, err := t.tx.Exec(isolationLevelQuery); err != nil {
		t.logger.Error("failed to set isolation level", zap.Error(err))
		return err
	}
	t.logger.Info("isolation level set", zap.String("isolation_level", level.String()))
	t.printLevel()
	return nil
}

func (t *transaction) printLevel() error {
	var isolationLevelQuery = "SHOW transaction_isolation;"
	var isolationLevel string
	if err := t.tx.QueryRow(isolationLevelQuery).Scan(&isolationLevel); err != nil {
		t.logger.Error("failed to get isolation level", zap.Error(err))
		return err
	}
	t.logger.Info("isolation level", zap.String("isolation_level", isolationLevel))
	return nil
}

func (t *transaction) updateUser(id, balance int) error {
	const updateQuery = "UPDATE person SET balance = $1 WHERE id = $2;"
	if _, err := t.tx.Exec(updateQuery, balance, id); err != nil {
		t.logger.Error("failed to update balance", zap.Error(err), zap.Int("balance", balance))
		return err
	}
	t.logger.Info("balance updated", zap.Int("balance", balance), zap.Int("id", id))
	return nil
}

func (t *transaction) insertUser(id, balance int) error {
	const insertQuery = "INSERT INTO person VALUES ($1, $2);"
	if _, err := t.tx.Exec(insertQuery, id, balance); err != nil {
		t.logger.Error("failed to insert user", zap.Error(err), zap.Int("id", id), zap.Int("balance", balance))
		return err
	}
	t.logger.Info("user inserted", zap.Int("id", id), zap.Int("balance", balance))
	return nil
}

func (t *transaction) printUsersCount() error {
	const readQuery = "SELECT COUNT(*) FROM person;"
	var count int
	if err := t.tx.QueryRow(readQuery).Scan(&count); err != nil {
		t.logger.Error("failed to get count", zap.Error(err))
		return err
	}
	t.logger.Info("count read", zap.Int("count", count))
	return nil
}

func (t *transaction) printUserBalance(id int) error {
	const readQuery = "SELECT balance FROM person WHERE id = $1;"
	var balance int
	if err := t.tx.QueryRow(readQuery, id).Scan(&balance); err != nil {
		t.logger.Error("failed to get balance", zap.Error(err), zap.Int("id", id))
		return err
	}
	t.logger.Info("balance read", zap.Int("balance", balance), zap.Int("id", id))
	return nil
}

func (t *transaction) deleteUser(id int) error {
	const deleteQuery = "DELETE FROM person WHERE id = $1;"
	if _, err := t.tx.Exec(deleteQuery, id); err != nil {
		t.logger.Error("failed to delete user", zap.Error(err), zap.Int("id", id))
		return err
	}
	t.logger.Info("user deleted", zap.Int("id", id))
	return nil
}

func (t *transaction) rollback() error {
	if err := t.tx.Rollback(); err != nil {
		t.logger.Error("failed to rollback tx", zap.Error(err))
		return err
	}
	t.logger.Info("tx rolled back")
	return nil
}

func (t *transaction) commit() error {
	if err := t.tx.Commit(); err != nil {
		t.logger.Error("failed to commit tx", zap.Error(err))
		return err
	}
	t.logger.Info("tx committed")
	return nil
}

type isolationProblem func(db *sqlx.DB, logger *zap.Logger) error

var isolationProblems = map[string]isolationProblem{
	//"dirty_read":          dirtyRead,
	//"non_repeatable_read": nonRepeatableRead,
	"phantom_read": phantomRead,
	//"lost_update":         lostUpdate,
}

func main() {
	logger, err := zap.NewDevelopment(
		zap.WithCaller(false),
		zap.AddStacktrace(zap.FatalLevel),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer logger.Sync()

	db, err := connect(logger)
	if err != nil {
		log.Fatalln(err)
	}
	for name, problem := range isolationProblems {
		if err = migrate(db, logger.With(zap.String("problem", name))); err != nil {
			log.Fatalln(err)
		}
		if err = problem(db, logger.With(zap.String("problem", name))); err != nil {
			log.Fatalln(err)
		}
	}
}

func phantomRead(db *sqlx.DB, logger *zap.Logger) error {
	// Проверка количества записей после завершения транзакций
	defer func() {
		tx3Logger := logger.With(zap.String("tx", "tx3"))
		tx3 := newTransaction(db, tx3Logger)
		if err := tx3.begin(); err != nil {
			return
		}
		if err := tx3.printUsersCount(); err != nil {
			return
		}
		if err := tx3.commit(); err != nil {
			return
		}
	}()

	// Запуск первой транзакции
	tx1Logger := logger.With(zap.String("tx", "tx1"))
	tx1 := newTransaction(db, tx1Logger)
	if err := tx1.begin(); err != nil {
		return err
	}
	if err := tx1.setLevel(sql.LevelReadCommitted); err != nil {
		return err
	}
	// Запуск второй транзакции
	tx2Logger := logger.With(zap.String("tx", "tx2"))
	tx2 := newTransaction(db, tx2Logger)
	if err := tx2.begin(); err != nil {
		return err
	}
	if err := tx2.setLevel(sql.LevelReadCommitted); err != nil {
		return err
	}

	// Чтение количества записей в 1 транзакции
	if err := tx1.printUsersCount(); err != nil {
		return err
	}

	// Добавление записи во 2 транзакции
	if err := tx2.insertUser(3, 1000); err != nil {
		return err
	}
	if err := tx2.commit(); err != nil {
		return err
	}

	// Чтение количества записей в 1 транзакции
	if err := tx1.printUsersCount(); err != nil {
		return err
	}
	if err := tx1.commit(); err != nil {
		return err
	}
	return nil
}

func nonRepeatableRead(db *sqlx.DB, logger *zap.Logger) error {
	// Проверка баланса после завершения транзакций
	defer func() {
		tx3Logger := logger.With(zap.String("tx", "tx3"))
		tx3 := newTransaction(db, tx3Logger)
		if err := tx3.begin(); err != nil {
			return
		}
		if err := tx3.printUserBalance(1); err != nil {
			return
		}
		if err := tx3.commit(); err != nil {
			return
		}
	}()

	// Запуск первой транзакции
	tx1Logger := logger.With(zap.String("tx", "tx1"))
	tx1 := newTransaction(db, tx1Logger)
	if err := tx1.begin(); err != nil {
		return err
	}
	if err := tx1.setLevel(sql.LevelReadCommitted); err != nil {
		return err
	}
	// Запуск второй транзакции
	tx2Logger := logger.With(zap.String("tx", "tx2"))
	tx2 := newTransaction(db, tx2Logger)
	if err := tx2.begin(); err != nil {
		return err
	}
	if err := tx2.setLevel(sql.LevelReadCommitted); err != nil {
		return err
	}

	// Чтение баланса в 1 транзакции
	userID := 1
	newBalance1 := 100_000
	if err := tx1.printUserBalance(userID); err != nil {
		return err
	}

	// Обновление баланса во 2 транзакции
	if err := tx2.updateUser(userID, newBalance1); err != nil {
		return err
	}
	if err := tx2.commit(); err != nil {
		return err
	}

	// Чтение баланса в 1 транзакции
	if err := tx1.printUserBalance(userID); err != nil {
		return err
	}
	if err := tx1.commit(); err != nil {
		return err
	}
	return nil
}

func dirtyRead(db *sqlx.DB, logger *zap.Logger) error {
	// Проверка баланса после завершения транзакций
	defer func() {
		tx3Logger := logger.With(zap.String("tx", "tx3"))
		tx3 := newTransaction(db, tx3Logger)
		if err := tx3.begin(); err != nil {
			return
		}
		if err := tx3.printUserBalance(1); err != nil {
			return
		}
		if err := tx3.commit(); err != nil {
			return
		}
	}()

	// Запуск первой транзакции
	tx1Logger := logger.With(zap.String("tx", "tx1"))
	tx1 := newTransaction(db, tx1Logger)
	if err := tx1.begin(); err != nil {
		return err
	}
	if err := tx1.setLevel(sql.LevelReadUncommitted); err != nil {
		return err
	}

	// Запуск второй транзакции
	tx2Logger := logger.With(zap.String("tx", "tx2"))
	tx2 := newTransaction(db, tx2Logger)
	if err := tx2.begin(); err != nil {
		return err
	}
	if err := tx2.setLevel(sql.LevelReadUncommitted); err != nil {
		return err
	}

	// Обновление баланса в 1 транзакции
	newBalance := 100_000
	userID := 1
	if err := tx1.updateUser(userID, newBalance); err != nil {
		return err
	}

	// Чтение баланса во 2 транзакции
	if err := tx2.printUserBalance(userID); err != nil {
		return err
	}

	// Откат первой транзакции
	if err := tx1.rollback(); err != nil {
		return err
	}
	if err := tx2.commit(); err != nil {
		return err
	}
	return nil
}

func lostUpdate(db *sqlx.DB, logger *zap.Logger) error {
	// Проверка баланса после завершения транзакций
	defer func() {
		tx3Logger := logger.With(zap.String("tx", "tx3"))
		tx3 := newTransaction(db, tx3Logger)
		if err := tx3.begin(); err != nil {
			return
		}
		if err := tx3.printUserBalance(1); err != nil {
			return
		}
		if err := tx3.commit(); err != nil {
			return
		}
	}()

	// Запуск первой транзакции
	tx1Logger := logger.With(zap.String("tx", "tx1"))
	tx1 := newTransaction(db, tx1Logger)
	if err := tx1.begin(); err != nil {
		return err
	}
	if err := tx1.setLevel(sql.LevelReadCommitted); err != nil {
		return err
	}

	// Запуск второй транзакции
	tx2Logger := logger.With(zap.String("tx", "tx2"))
	tx2 := newTransaction(db, tx2Logger)
	if err := tx2.begin(); err != nil {
		return err
	}
	if err := tx2.setLevel(sql.LevelReadCommitted); err != nil {
		return err
	}

	// Чтение баланса
	userID := 1
	if err := tx1.printUserBalance(userID); err != nil {
		return err
	}
	if err := tx2.printUserBalance(userID); err != nil {
		return err
	}

	// Обновление баланса в 1 транзакции
	newBalance1 := 100_000
	if err := tx1.updateUser(userID, newBalance1); err != nil {
		return err
	}
	if err := tx1.commit(); err != nil {
		return err
	}

	// Обновление баланса во 2 транзакции
	newBalance2 := 10
	if err := tx2.updateUser(userID, newBalance2); err != nil {
		return err
	}
	if err := tx2.commit(); err != nil {
		return err
	}
	return nil
}
