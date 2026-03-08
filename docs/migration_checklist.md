# Migration Checklist: DLT to Databricks Pipelines

Use this checklist when migrating from @dlt to @dp syntax.

## Pre-Migration

- [ ] Read the introduction documentation
- [ ] Review syntax reference guide
- [ ] Backup existing DLT pipelines
- [ ] Create test environment for validation
- [ ] Identify all pipelines to migrate

## Step 1: Convert Notebooks to Files

- [ ] Create `src/` directory structure
- [ ] Create subdirectories: `bronze/`, `silver/`, `gold/`
- [ ] Copy notebook code to `.py` files
- [ ] Remove notebook-specific code (display, %sql, etc.)
- [ ] Organize by layer and business entity

## Step 2: Update Import Statements

- [ ] Replace `import dlt` with `from pyspark import pipelines as dp`
- [ ] Update all decorator references
- [ ] Fix any dlt.read() calls to spark.read.table()

## Step 3: Update Decorator Syntax

- [ ] Convert `@dlt.table(streaming=True)` → `@dp.table`
- [ ] Convert `@dlt.table()` → `@dp.materialized_view`  
- [ ] Keep expectations as-is (they're compatible)
- [ ] Update CDC: `dlt.apply_changes()` → `dp.create_auto_cdc_flow()`

## Step 4: Define Explicit Schemas

- [ ] Add StructType schema definitions for all Auto Loader sources
- [ ] Remove schema inference where possible
- [ ] Add schemaHints for complex types
- [ ] Document schema version in comments

## Step 5: Separate Concerns

- [ ] Split monolithic transformations into layers
- [ ] Bronze: Raw ingestion only
- [ ] Silver: Cleaning and validation
- [ ] Gold: Business aggregations
- [ ] Ensure clear separation

## Step 6: Add Data Quality Rules

- [ ] Define quality expectations for each table
- [ ] Use `@dp.expect` for monitoring
- [ ] Use `@dp.expect_or_drop` for cleaning
- [ ] Use `@dp.expect_or_fail` for critical rules
- [ ] Document quality rules in comments

## Step 7: Test Migration

- [ ] Create test pipeline in new environment
- [ ] Run with small dataset first
- [ ] Validate output matches original
- [ ] Check data quality metrics
- [ ] Verify lineage in Unity Catalog

## Step 8: Performance Optimization

- [ ] Enable `pipelines.autoOptimize.managed = true`
- [ ] Review partitioning strategies
- [ ] Optimize join operations
- [ ] Consider serverless compute
- [ ] Monitor execution metrics

## Step 9: Documentation

- [ ] Add comments to all table definitions
- [ ] Document data quality rules
- [ ] Update README with pipeline structure
- [ ] Create runbook for operations
- [ ] Document dependencies

## Step 10: Deployment

- [ ] Deploy to production environment
- [ ] Configure monitoring and alerts
- [ ] Set up backup/recovery procedures
- [ ] Train team on new syntax
- [ ] Archive old DLT pipelines

## Post-Migration Validation

- [ ] Compare data volumes before/after
- [ ] Verify data quality metrics
- [ ] Check execution times
- [ ] Review cost metrics
- [ ] Collect user feedback

## Common Pitfalls to Avoid

❌ **Don't:**
- Use action methods (.collect(), .save()) in dataset definitions
- Mix bronze/silver/gold logic in one file
- Skip explicit schema definitions
- Forget to test with production data volumes
- Deploy without backup plan

✅ **Do:**
- Use separate files for each table/view
- Define explicit schemas
- Implement comprehensive quality checks
- Test thoroughly before production
- Monitor execution and costs

## Rollback Plan

If migration fails:

1. [ ] Identify the failure point
2. [ ] Revert to previous DLT pipeline
3. [ ] Document the issue
4. [ ] Fix in test environment
5. [ ] Re-attempt migration

## Success Criteria

Migration is complete when:

- [ ] All pipelines running successfully with @dp syntax
- [ ] Data quality matches or exceeds previous
- [ ] Performance is comparable or better
- [ ] Team trained on new syntax
- [ ] Documentation updated
- [ ] Old DLT pipelines archived

---

**Timeline Estimate:**
- Small pipeline (< 10 tables): 1-2 days
- Medium pipeline (10-50 tables): 1 week
- Large pipeline (50+ tables): 2-4 weeks

**Resources Needed:**
- Data engineer for migration
- Databricks workspace with Lakeflow
- Test environment
- Production deployment window

---

For questions or issues, refer to:
- Syntax Reference Guide
- Code examples in `src/`
- Databricks documentation
