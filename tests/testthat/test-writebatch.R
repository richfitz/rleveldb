context("writebatch")

test_that("create", {
  wb <- leveldb_writebatch_create()
  expect_true(leveldb_writebatch_destroy(wb))
  expect_false(leveldb_writebatch_destroy(wb))
  expect_error(leveldb_writebatch_destroy(wb, TRUE),
               "leveldb writebatch is not open")
})

## I might change the lifecycle here so that writebatch's *can* come
## from nowhere (so that one can drive this from the db or from
## nowhere and apply).  Bind the db into the writebatch and require it
## if it's not there.
test_that("basic", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  wb <- db$writebatch()
  expect_is(wb, "leveldb_writebatch")
  expect_is(wb, "R6")
  ## clear chains:
  expect_identical(wb$clear(), wb)
  wb$put("foo", "bar")
  wb$write()
  expect_true(db$exists("foo"))
  expect_equal(db$get("foo"), "bar")
})

test_that("destroy", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  wb <- db$writebatch()
  expect_true(wb$destroy())
  expect_false(wb$destroy())
  expect_error(wb$destroy(TRUE),
               "leveldb writebatch is not open")
})

test_that("bulk write", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  wb <- db$writebatch()
  k <- unique(replicate(50, rand_str(rpois(1, 5))))
  v <- replicate(length(k), rand_str(rpois(1, 5)))
  for (i in seq_along(k)) {
    wb$put(k[[i]], v[[i]])
  }

  expect_equal(db$keys_len(), 0)
  wb$write(leveldb_writeoptions(sync = TRUE))
  expect_equal(db$keys_len(), length(k))
  expect_true(all(k %in% db$keys()))
})

test_that("bulk delete", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  k <- unique(replicate(50, rand_str(rpois(1, 5))))
  v <- replicate(length(k), rand_str(rpois(1, 5)))
  for (i in seq_along(k)) {
    db$put(k[[i]], v[[i]])
  }

  wb <- db$writebatch()
  k2 <- sample(k, ceiling(length(k) / 2))
  for (i in k2) {
    wb$delete(i)
  }

  expect_equal(db$keys_len(), length(k))
  wb$write(leveldb_writeoptions(sync = TRUE))
  expect_equal(db$keys_len(), length(k) - length(k2))
  expect_false(any(k2 %in% db$keys()))
})

test_that("clear", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())
  db$writebatch()$put("foo", "bar")$clear()$write()
  expect_false(db$exists("foo"))
  db$writebatch()$put("foo", "bar")$write()
  expect_true(db$exists("foo"))
})
