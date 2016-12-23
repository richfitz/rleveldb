context("iterators")

test_that("iterator", {
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
})
