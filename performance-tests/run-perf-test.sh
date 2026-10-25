#!/bin/bash
set -e

echo "=== High-Scale Order Platform Performance Tests ==="

APP_URL=${APP_URL:-http://localhost:8080}

echo "Checking app health..."
curl -s $APP_URL/actuator/health || { echo "App not healthy at $APP_URL"; exit 1; }

echo "Compiling performance test..."
javac PerformanceTest.java -d .

echo ""
echo "Test 1: Product Browsing (Read-Heavy) - 20 users, 30s"
java -cp . com.ordersystem.perftest.PerformanceTest --url $APP_URL/api/products --users 20 --duration 30

echo ""
echo "Test 2: Product Browsing - 50 users, 60s"
java -cp . com.ordersystem.perftest.PerformanceTest --url $APP_URL/api/products --users 50 --duration 60

echo ""
echo "Test 3: Single Product - 20 users, 30s"
java -cp . com.ordersystem.perftest.PerformanceTest --url $APP_URL/api/products/1 --users 20 --duration 30

echo ""
echo "=== All tests completed ==="
echo "Document results in README.md Performance Testing section"
echo "Include environment details: CPU, RAM, Docker resources, etc."
