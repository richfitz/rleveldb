context("rleveldb - R6")

test_that("creation", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  expect_is(db, "leveldb")
  expect_is(db, "R6")
  expect_is(db$db, "leveldb_connection")
})

test_that("property", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  ## Full list of properties:
  ##
  ## leveldb.num-files-at-level*
  ## leveldb.stats
  ## leveldb.sstables
  ## leveldb.approximate-memory-usage
  expect_is(db$property("leveldb.stats"), "character")
  expect_null(db$property("nosuch"))
  expect_error(db$property("nosuch", TRUE), "No such property 'nosuch'")
})

test_that("put, get (basic)", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  expect_null(db$put("foo", "bar"))
  expect_equal(db$get("foo"), "bar")
})

test_that("put, get (raw key)", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  k <- as.raw(c(5L, 0L, 255L))
  expect_null(db$put(k, "bar"))
  expect_equal(db$get(k), "bar")
  ## So, this is not ideal:
  expect_error(db$keys(), "embedded nul in string")
  expect_equal(db$keys(TRUE), list(k))
})

test_that("put, get (raw value)", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  v <- as.raw(c(5L, 0L, 255L))
  expect_null(db$put("foo", v))
  ## TODO: fix this so that this does return 'v'?  Might not be ideal.
  ## In any case it should probably not throw the error that is thrown
  ##   expect_equal(db$get("foo"), v)
  expect_equal(db$get("foo", TRUE), v)
})

test_that("put, with writeoptions", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  options <- leveldb_writeoptions(TRUE)
  ## We won't know that this definitely worked, unfortunately...
  expect_null(db$put("foo", "bar", options))
  expect_equal(db$get("foo"), "bar")
})

test_that("get, with readoptions", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  options <- leveldb_readoptions(verify_checksums = TRUE)
  expect_null(db$put("foo", "bar"))
  ## We won't know that this definitely worked, unfortunately...
  expect_equal(db$get("foo", readoptions = options), "bar")
})

test_that("delete", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  db$put("foo", "bar")
  expect_true(db$exists("foo"))
  expect_null(db$delete("foo"))
  expect_false(db$exists("foo"))
  expect_null(db$delete("foo"))
})

test_that("keys, keys_len", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  expect_equal(db$keys_len(), 0L)
  expect_equal(db$keys(), character(0))
  expect_equal(db$keys(TRUE), list())

  k <- unique(replicate(50, rand_str(rpois(1, 5))))
  v <- replicate(length(k), rand_str(rpois(1, 5)))
  for (i in seq_along(k)) {
    db$put(k[[i]], v[[i]])
  }

  expect_equal(db$keys_len(), length(k))
  ## The keys *are* sorted on return, but not necessarily in the that
  ## R would sort them; they're sorted by byte order and that's not
  ## straightforward to compute in R space.
  expect_equal(sort(db$keys()), sort(k))

  dat <- db$keys(TRUE)
  expect_is(dat, "list")
  expect_true(all(vapply(dat, is.raw, logical(1))))
  expect_equal(vapply(dat, rawToChar, character(1)), db$keys())
})

test_that("approximate_sizes", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  expect_equal(db$approximate_sizes("a", "f"), 0)
  expect_equal(db$approximate_sizes(c("a", "f"), c("f", "z")),
               c(0, 0))

  ## TODO: this is possibly not actually working when there are any
  ## keys in the database!  I'm doing something wrong e.g., in the
  ## example above:
  ##
  ## db$approximate_sizes(dat[[1]], dat[[50]]) # 0!
})

test_that("compact_range", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  expect_null(db$compact_range(as.raw(0), as.raw(255)))
})
