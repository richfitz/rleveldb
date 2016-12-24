context("options")

test_that("readoptions - default", {
  opt <- leveldb_readoptions()
  expect_is(opt, "leveldb_readoptions")
  expect_is(opt, "leveldb_options")
  expect_equal(names(opt), names(formals(leveldb_readoptions)))

  expect_null(opt$verify_checksums)
  expect_null(opt$fill_cache)
  expect_null(opt$snapshot)

  expect_null(opt[["verify_checksums"]])
  expect_null(opt[["fill_cache"]])
  expect_null(opt[["snapshot"]])

  expect_error(opt$verify_checksums <- TRUE,
               "leveldb_readoptions objects are immutable")
  expect_error(opt$fill_cache <- TRUE,
               "leveldb_readoptions objects are immutable")
  expect_error(opt$snapshot <- TRUE,
               "leveldb_readoptions objects are immutable")

  expect_error(opt[["verify_checksums"]] <- TRUE,
               "leveldb_readoptions objects are immutable")

  expect_output(print(opt), "<leveldb_readoptions>", fixed = TRUE)
  expect_output(print(opt), "fill_cache: <not set>", fixed = TRUE)
})

test_that("readoptions - set", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  ss <- leveldb_snapshot(db)

  opt <- leveldb_readoptions(TRUE, TRUE, ss)
  expect_is(opt, "leveldb_readoptions")
  expect_is(opt, "leveldb_options")
  expect_equal(names(opt), names(formals(leveldb_readoptions)))

  expect_true(opt$verify_checksums)
  expect_true(opt$fill_cache)
  expect_identical(opt$snapshot, ss)

  expect_true(opt[["verify_checksums"]])
  expect_true(opt[["fill_cache"]])
  expect_identical(opt[["snapshot"]], ss)

  expect_output(print(opt), "fill_cache: TRUE", fixed = TRUE)
  expect_output(print(opt), "snapshot: <leveldb_snapshot>", fixed = TRUE)

  ## Just because this needs testing somewhere
  expect_output(print(ss), "<leveldb_snapshot> @", fixed = TRUE)
})

test_that("writeoptions - default", {
  opt <- leveldb_writeoptions()
  expect_is(opt, "leveldb_writeoptions")
  expect_is(opt, "leveldb_options")
  expect_equal(names(opt), names(formals(leveldb_writeoptions)))

  expect_null(opt$sync)

  expect_null(opt[["sync"]])

  expect_error(opt$sync <- TRUE,
               "leveldb_writeoptions objects are immutable")

  expect_output(print(opt), "<leveldb_writeoptions>", fixed = TRUE)
  expect_output(print(opt), "sync: <not set>", fixed = TRUE)
})

test_that("writeoptions - set", {
  opt <- leveldb_writeoptions(TRUE)
  expect_is(opt, "leveldb_writeoptions")
  expect_is(opt, "leveldb_options")
  expect_equal(names(opt), names(formals(leveldb_writeoptions)))

  expect_true(opt$sync)

  expect_true(opt[["sync"]])

  expect_error(opt$sync <- TRUE,
               "leveldb_writeoptions objects are immutable")

  expect_output(print(opt), "<leveldb_writeoptions>", fixed = TRUE)
  expect_output(print(opt), "sync: TRUE", fixed = TRUE)
})
