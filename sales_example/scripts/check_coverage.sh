#!/bin/bash
# Script to check dbt test coverage
# Usage: ./scripts/check_coverage.sh

echo "=== dbt Test Coverage Report ==="
echo ""

# Count total models
TOTAL_MODELS=$(dbt list --select resource_type:model --output name | wc -l)
echo "Total Models: $TOTAL_MODELS"

# Count models with tests
MODELS_WITH_TESTS=$(dbt list --select resource_type:model --output name | while read model; do
    TEST_COUNT=$(dbt list --select "test_type:data" --select "$model" --output name 2>/dev/null | wc -l)
    if [ "$TEST_COUNT" -gt 0 ]; then
        echo "$model"
    fi
done | wc -l)

echo "Models with Tests: $MODELS_WITH_TESTS"

# Calculate coverage percentage
if [ "$TOTAL_MODELS" -gt 0 ]; then
    COVERAGE=$(echo "scale=2; $MODELS_WITH_TESTS * 100 / $TOTAL_MODELS" | bc)
    echo "Test Coverage: ${COVERAGE}%"
else
    echo "Test Coverage: 0%"
fi

echo ""
echo "=== Models without tests ==="
dbt list --select resource_type:model --output name | while read model; do
    TEST_COUNT=$(dbt list --select "test_type:data" --select "$model" --output name 2>/dev/null | wc -l)
    if [ "$TEST_COUNT" -eq 0 ]; then
        echo "  - $model"
    fi
done

