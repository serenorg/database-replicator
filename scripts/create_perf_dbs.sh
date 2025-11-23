#!/bin/bash
# Helper script to create performance test databases for MongoDB and MySQL
# SQLite databases are created programmatically in the test suite

set -e

echo "========================================="
echo "Performance Test Database Setup"
echo "========================================="
echo ""

# Check if MongoDB URL is set
if [ -z "$TEST_MONGODB_URL" ]; then
    echo "⚠️  TEST_MONGODB_URL not set - skipping MongoDB setup"
    echo "   Set it to create MongoDB performance test databases"
    SKIP_MONGODB=true
else
    echo "✓ TEST_MONGODB_URL is set"
    SKIP_MONGODB=false
fi

# Check if MySQL URL is set
if [ -z "$TEST_MYSQL_URL" ]; then
    echo "⚠️  TEST_MYSQL_URL not set - skipping MySQL setup"
    echo "   Set it to create MySQL performance test databases"
    SKIP_MYSQL=true
else
    echo "✓ TEST_MYSQL_URL is set"
    SKIP_MYSQL=false
fi

echo ""

# ============================================================================
# MongoDB Performance Test Databases
# ============================================================================

if [ "$SKIP_MONGODB" = false ]; then
    echo "Creating MongoDB performance test databases..."
    echo ""

    # Extract MongoDB connection details
    MONGO_HOST=$(echo $TEST_MONGODB_URL | sed -E 's/mongodb:\/\/([^:]+:[^@]+@)?([^:\/]+).*/\2/')
    MONGO_PORT=$(echo $TEST_MONGODB_URL | sed -E 's/mongodb:\/\/[^:]+:([0-9]+).*/\1/')
    MONGO_USER=$(echo $TEST_MONGODB_URL | sed -E 's/mongodb:\/\/([^:]+):.*/\1/')
    MONGO_PASS=$(echo $TEST_MONGODB_URL | sed -E 's/mongodb:\/\/[^:]+:([^@]+)@.*/\1/')

    echo "  Creating 'small_test' database (1,000 documents)..."
    mongosh "$TEST_MONGODB_URL" --quiet <<'EOF'
use small_test;
db.users.drop();

// Insert 1,000 documents
for (let i = 0; i < 1000; i++) {
    db.users.insertOne({
        id: i,
        name: `User ${i}`,
        email: `user${i}@example.com`,
        age: 20 + (i % 50),
        balance: 100.0 + (i * 0.5),
        bio: `Biography for user ${i} with some text to make it larger`,
        created_at: new Date()
    });
}

print(`✓ Created small_test database with ${db.users.countDocuments()} documents`);
EOF

    echo "  Creating 'medium_test' database (50,000 documents)..."
    mongosh "$TEST_MONGODB_URL" --quiet <<'EOF'
use medium_test;
db.events.drop();

// Insert 50,000 documents in batches
const batchSize = 1000;
for (let batch = 0; batch < 50; batch++) {
    const docs = [];
    for (let i = 0; i < batchSize; i++) {
        const id = batch * batchSize + i;
        docs.push({
            id: id,
            user_id: id % 10000,
            event_type: `event_type_${id % 10}`,
            timestamp: new Date(1700000000000 + id * 1000),
            data: `Data for event ${id} with longer text content to increase size`,
            metadata: { batch: batch, index: i }
        });
    }
    db.events.insertMany(docs);
    if (batch % 10 === 0) {
        print(`  Progress: ${(batch + 1) * batchSize} documents inserted...`);
    }
}

print(`✓ Created medium_test database with ${db.events.countDocuments()} documents`);
EOF

    echo "✓ MongoDB performance test databases created"
    echo ""
fi

# ============================================================================
# MySQL Performance Test Databases
# ============================================================================

if [ "$SKIP_MYSQL" = false ]; then
    echo "Creating MySQL performance test databases..."
    echo ""

    # Extract MySQL connection details
    MYSQL_HOST=$(echo $TEST_MYSQL_URL | sed -E 's/mysql:\/\/([^:]+:[^@]+@)?([^:\/]+).*/\2/')
    MYSQL_PORT=$(echo $TEST_MYSQL_URL | sed -E 's/mysql:\/\/[^:]+:([0-9]+).*/\1/')
    MYSQL_USER=$(echo $TEST_MYSQL_URL | sed -E 's/mysql:\/\/([^:]+):.*/\1/')
    MYSQL_PASS=$(echo $TEST_MYSQL_URL | sed -E 's/mysql:\/\/[^:]+:([^@]+)@.*/\1/')

    echo "  Creating 'small_test' database (1,000 rows)..."
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASS" <<'EOF'
DROP DATABASE IF EXISTS small_test;
CREATE DATABASE small_test;
USE small_test;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INT,
    balance DECIMAL(10,2),
    bio TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Insert 1,000 rows
INSERT INTO users (id, name, email, age, balance, bio)
SELECT
    n,
    CONCAT('User ', n),
    CONCAT('user', n, '@example.com'),
    20 + MOD(n, 50),
    100.0 + (n * 0.5),
    CONCAT('Biography for user ', n, ' with some text to make it larger')
FROM (
    SELECT @row := @row + 1 AS n
    FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
         (SELECT @row := 0) r
    LIMIT 1000
) nums;

SELECT CONCAT('✓ Created small_test database with ', COUNT(*), ' rows') FROM users;
EOF

    echo "  Creating 'medium_test' database (50,000 rows)..."
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASS" <<'EOF'
DROP DATABASE IF EXISTS medium_test;
CREATE DATABASE medium_test;
USE medium_test;

CREATE TABLE events (
    id INT PRIMARY KEY,
    user_id INT,
    event_type VARCHAR(50),
    timestamp BIGINT,
    data TEXT,
    metadata TEXT
);

-- Insert 50,000 rows
INSERT INTO events (id, user_id, event_type, timestamp, data, metadata)
SELECT
    n,
    MOD(n, 10000),
    CONCAT('event_type_', MOD(n, 10)),
    1700000000 + n,
    CONCAT('Data for event ', n, ' with longer text content to increase size'),
    CONCAT('Metadata for event ', n)
FROM (
    SELECT @row := @row + 1 AS n
    FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t4,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t5,
         (SELECT @row := 0) r
    LIMIT 50000
) nums;

SELECT CONCAT('✓ Created medium_test database with ', COUNT(*), ' rows') FROM events;
EOF

    echo "✓ MySQL performance test databases created"
    echo ""
fi

# ============================================================================
# Summary
# ============================================================================

echo "========================================="
echo "Performance Test Database Setup Complete"
echo "========================================="
echo ""
echo "SQLite databases: Created programmatically by test suite"
if [ "$SKIP_MONGODB" = false ]; then
    echo "MongoDB databases: ✓ small_test, medium_test"
else
    echo "MongoDB databases: ⚠️  Skipped (TEST_MONGODB_URL not set)"
fi
if [ "$SKIP_MYSQL" = false ]; then
    echo "MySQL databases: ✓ small_test, medium_test"
else
    echo "MySQL databases: ⚠️  Skipped (TEST_MYSQL_URL not set)"
fi
echo ""
echo "Run performance tests with:"
echo "  cargo test --release --test performance_test -- --ignored --nocapture"
echo ""
