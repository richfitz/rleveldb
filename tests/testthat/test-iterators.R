context("iterators")

test_that("basic", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  k <- replicate(50, rand_str(rpois(1, 5)))
  v <- replicate(length(k), rand_str(rpois(1, 5)))
  for (i in seq_along(k)) {
    db$put(k[[i]], v[[i]])
  }

  i <- key_order(k)
  kk <- k[i]
  vv <- v[i]

  it <- db$iterator()
  expect_is(it, "leveldb_iterator")
  expect_false(it$valid())

  expect_identical(it$seek_to_first(), it)
  expect_true(it$valid())
  expect_equal(it$key(), kk[[1L]])
  expect_equal(it$value(), vv[[1L]])

  it$move_next()
  expect_true(it$valid())
  expect_equal(it$key(), kk[[2L]])
  expect_equal(it$value(), vv[[2L]])

  it$move_prev()
  expect_true(it$valid())
  expect_equal(it$key(), kk[[1L]])
  expect_equal(it$value(), vv[[1L]])

  ## NOTE: method chaining:
  expect_equal(it$seek(k[[10]])$key(), k[[10]])
  expect_equal(it$value(), v[[10]])

  it$seek_to_last()
  expect_true(it$valid())
  expect_equal(it$key(), kk[[length(kk)]])
  expect_equal(it$value(), vv[[length(vv)]])

  it$move_next()
  expect_false(it$valid())
  ## keep going, no crash
  it$move_next()
  expect_false(it$valid())
  ## try to reverse, but no joy
  it$move_prev()
  expect_false(it$valid())
  ## can jump back to the end though
  it$seek_to_last()
  expect_true(it$valid())

  expect_true(it$destroy())
  expect_error(it$valid(), "leveldb iterator is not open")
  expect_false(it$destroy())
  expect_error(it$destroy(TRUE), "leveldb iterator is not open")
})

test_that("snapshot", {
  db <- leveldb(tempfile(), create_if_missing = TRUE)
  on.exit(db$destroy())

  k <- replicate(50, rand_str(rpois(1, 5)))
  v <- replicate(length(k), rand_str(rpois(1, 5)))
  i <- seq_len(floor(length(k) / 2))

  k1 <- k[i]
  v1 <- v[i]
  k2 <- k[-i]
  v2 <- v[-i]

  for (i in seq_along(k1)) {
    db$put(k1[[i]], v1[[i]])
  }

  ## TODO: test to see what happens if the snapshot has been released here
  snapshot <- db$snapshot()
  options <- leveldb_readoptions(snapshot = snapshot)
  it <- db$iterator(options)

  f <- function(it) {
    res <- list()
    it$seek_to_first()
    while (it$valid()) {
      res[[it$key()]] <- it$value()
      it$move_next()
    }
    res
  }

  res <- f(it)
  expect_equal(names(res), db$keys())

  ## Now, stuff some keys into the database:
  for (i in seq_along(k2)) {
    db$put(k2[[i]], v2[[i]])
  }

  ## Still the same:
  expect_equal(f(it), res)
  ## But more keys are in the database:
  expect_equal(db$keys_len(), length(k))

  ## Delete some keys:
  for (i in sample(length(k1), floor(length(k1) / 2))) {
    db$delete(k1[[i]])
  }

  expect_equal(f(it), res)
  expect_equal(db$keys_len(), length(k) - floor(length(k1) / 2))

  ## With our snapshot we can create new iterators:
  expect_equal(f(db$iterator(leveldb_readoptions(snapshot = snapshot))), res)
})
