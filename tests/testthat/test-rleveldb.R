context("rleveldb")

test_that("CRUD", {
  db <- rleveldb_connect(tempfile())
  rleveldb_put(db, "foo", "bar")
  expect_equal(rleveldb_get(db, "foo"), "bar")
  expect_null(rleveldb_delete(db, "foo"))

  rleveldb_put(db, "foo", charToRaw("barbar"))
  expect_equal(rleveldb_get(db, "foo"), "barbar")
  expect_equal(rleveldb_get(db, "foo", TRUE), charToRaw("barbar"))
})
