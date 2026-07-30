package gorm

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// DB GORM DB definition
type DB struct {
	*Config
	Error        error
	RowsAffected int64
	Statement    *Statement
	clone        int
}

// Session session config when create session with Session() method
type Session struct {
	DryRun                   bool
	PrepareStmt              bool
	NewDB                    bool
	Initialized              bool
	SkipHooks                bool
	SkipDefaultTransaction   bool
	DisableNestedTransaction bool
	AllowGlobalUpdate        bool
	FullSaveAssociations     bool
	QueryFields              bool
	Context                  context.Context
	Logger                   logger.Interface
	NowFunc                  func() time.Time
	CreateBatchSize          int
}

// Open initialize db session based on dialector
func Open(dialector Dialector, opts ...Option) (db *DB, err error) {
	config := &Config{}

	sort.Slice(opts, func(i, j int) bool {
		_, isConfig := opts[i].(*Config)
		_, isConfig2 := opts[j].(*Config)
		if isConfig && isConfig2 {
			return false
		}
		if isConfig {
			return true
		}
		return false
	})

	for _, opt := range opts {
		if opt != nil {
			if applyErr := opt.Apply(config); applyErr != nil {
				return nil, applyErr
			}
		}
	}

	if d, ok := dialector.(interface{ Apply(*Config) error }); ok {
		if err = d.Apply(config); err != nil {
			return nil, err
		}
	}

	if config.NamingStrategy == nil && config.SchemaStore == nil {
		config.NamingStrategy = schema.NamingStrategy{}
	}

	if config.Logger == nil {
		config.Logger = logger.Default
	}

	if config.NowFunc == nil {
		config.NowFunc = func() time.Time { return time.Now().Local() }
	}

	if config.Dialector == nil {
		config.Dialector = dialector
	}

	if config.Plugins == nil {
		config.Plugins = map[string]Plugin{}
	}

	if config.CreateBatchSize < 0 {
		config.CreateBatchSize = 0
	}

	db = &DB{Config: config, Error: schema.ErrUnsupportedDataType}
	db.Statement = &Statement{
		DB:       db,
		ConnPool: db.ConnPool,
		Context:  context.Background(),
		Clauses:  map[string]clause.Clause{},
	}

	if err = db.executeCallbacks(); err != nil {
		return
	}

	if db.Error != nil {
		return nil, db.Error
	}

	return db, nil
}

// Session create new db session
func (db *DB) Session(config *Session) *DB {
	var (
		tx       = db.getInstance()
		txConfig = *tx.Config
	)

	if config.Context != nil {
		tx.Statement.Context = config.Context
	}

	if config.Logger != nil {
		txConfig.Logger = config.Logger
	}

	if config.NowFunc != nil {
		txConfig.NowFunc = config.NowFunc
	}

	if config.CreateBatchSize != 0 {
		txConfig.CreateBatchSize = config.CreateBatchSize
	}

	txConfig.SkipDefaultTransaction = config.SkipDefaultTransaction
	txConfig.NamingStrategy = db.Config.NamingStrategy
	txConfig.FullSaveAssociations = config.FullSaveAssociations

	tx.Config = &txConfig
	tx.Statement.Clauses = map[string]clause.Clause{}
	tx.Statement.NewDB = config.NewDB
	tx.Statement.SkipHooks = config.SkipHooks
	tx.Statement.SQL = strings.Builder{}
	tx.Statement.Vars = nil

	if config.DisableNestedTransaction {
		tx.Config.DisableNestedTransaction = true
	}

	if config.AllowGlobalUpdate {
		tx.Config.AllowGlobalUpdate = true
	}

	if config.QueryFields {
		tx.Config.QueryFields = true
	}

	if config.PrepareStmt {
		if prepStmtConn, ok := tx.Statement.ConnPool.(*PreparedStmtTX); ok {
			tx.Statement.ConnPool = prepStmtConn.ConnPool
		}

		if db.cacheStore == nil {
			db.cacheStore = &sync.Map{}
		}

		tx.Statement.ConnPool = &PreparedStmtDB{
			ConnPool:    db.ConnPool,
			Mux:         &sync.RWMutex{},
			PreparedSQL: make(map[string]*sql.Stmt),
		}
		db.cacheStore.Store(preparedStmtDBKey, tx.Statement.ConnPool)
	} else if db.cacheStore != nil {
		if preparedStmtDB, ok := db.cacheStore.Load(preparedStmtDBKey); ok {
			tx.Statement.ConnPool = &PreparedStmtDB{
				ConnPool: db.ConnPool,
				Mux:      preparedStmtDB.(*PreparedStmtDB).Mux,
				PreparedSQL: preparedStmtDB.(*PreparedStmtDB).PreparedSQL,
			}
		}
	}

	tx.Statement.RaiseErrorOnNotFound = config.Initialized

	return tx
}

// WithContext change current db connection's context to ctx
func (db *DB) WithContext(ctx context.Context) *DB {
	return db.Session(&Session{Context: ctx})
}

// Debug start debug mode
func (db *DB) Debug() *DB {
	return db.Session(&Session{
		Logger: db.Logger.LogMode(logger.Info),
	})
}

// Set store value with key into current db instance's context
func (db *DB) Set(key string, value interface{}) *DB {
	tx := db.getInstance()
	tx.Statement.Settings.Store(key, value)
	return tx
}

// Get get value with key from current db instance's context
func (db *DB) Get(key string) (interface{}, bool) {
	return db.Statement.Settings.Load(key)
}

// InstanceSet store value with key into current db instance's context
func (db *DB) InstanceSet(key string, value interface{}) *DB {
	tx := db.getInstance()
	tx.Statement.Settings.Store(fmt.Sprintf("gorm:%s", key), value)
	return tx
}

// InstanceGet get value with key from current db instance's context
func (db *DB) InstanceGet(key string) (interface{}, bool) {
	return db.Statement.Settings.Load(fmt.Sprintf("gorm:%s", key))
}

// Callback returns callback manager
func (db *DB) Callback() *Callbacks {
	return db.callbacks
}

// AddError add error to db
func (db *DB) AddError(err error) error {
	if err != nil {
		if db.Error == nil {
			db.Error = err
		} else if err != context.Canceled && err != context.DeadlineExceeded && db.Error != context.Canceled && db.Error != context.DeadlineExceeded {
			db.Error = fmt.Errorf("%w; %v", db.Error, err)
		}
	}
	return db.Error
}

// DB returns `*sql.DB`
func (db *DB) DB() (*sql.DB, error) {
	if connPool, ok := db.Statement.ConnPool.(*sql.DB); ok {
		return connPool, nil
	}

	if connPool, ok := db.Statement.ConnPool.(ConnPoolValuer); ok {
		return connPool.Value(), nil
	}

	return nil, ErrInvalidDB
}

func (db *DB) getInstance() *DB {
	if db.clone > 0 {
		tx := &DB{Config: db.Config, Error: db.Error}

		if db.clone == 1 {
			// clone with new statement
			tx.Statement = &Statement{
				DB:       tx,
				ConnPool: db.Statement.ConnPool,
				Context:  db.Statement.Context,
				Clauses:  map[string]clause.Clause{},
				Vars:     make([]interface{}, 0, 8),
			}
		} else {
			// share statement
			tx.Statement = db.Statement
		}

		return tx
	}

	return db
}

// Transaction start a transaction as a block, return error will rollback, otherwise to commit.
func (db *DB) Transaction(fc func(tx *DB) error, opts ...*sql.TxOptions) (err error) {
	panicked := true

	if committer, ok := db.Statement.ConnPool.(TxCommitter); ok && committer != nil {
		// nested transaction
		if !db.DisableNestedTransaction {
			spName := fmt.Sprintf("sp%p", fc)
			err = db.SavePoint(spName).Error
			if err != nil {
				return err
			}
			defer func() {
				// Something database-specific might be needed here, but generally:
				if panicked || err != nil {
					if rollbackErr := db.RollbackTo(spName).Error; rollbackErr != nil {
						if err == nil {
							err = rollbackErr
						}
					}
				}
			}()
		}
		err = fc(db.Session(&Session{NewDB: db.Session.NewDB}))
		if err == nil && db.Statement.Context != nil && db.Statement.Context.Err() != nil {
			err = db.Statement.Context.Err()
		}
	} else {
		tx := db.Begin(opts...)
		if tx.Error != nil {
			return tx.Error
		}

		defer func() {
			// Committing/rolling back the transaction
			if panicked || err != nil {
				tx.Rollback()
			}
		}()

		if err = fc(tx); err == nil {
			if tx.Statement.Context != nil && tx.Statement.Context.Err() != nil {
				err = tx.Statement.Context.Err()
			} else {
				panicked = false
				return tx.Commit().Error
			}
		}
	}

	panicked = false
	return err
}
