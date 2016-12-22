rand_str <- function(len = 8, prefix = "") {
  paste0(prefix,
         paste(sample(c(LETTERS, letters, 0:9), len), collapse = ""))
}

rand_bytes <- function(len) {
  sample(as.raw(0:255), len, TRUE)
}

key_order <- function(x) {
  if (is.character(x)) {
    x <- lapply(x, charToRaw)
  }
  n <- lengths(x)
  m <- max(n)
  pad <- function(el, len) {
    if (length(el) < len) {
      el <- c(el, rep(as.raw(0), len - length(el)))
    }
    as.integer(el)
  }
  tmp <- vapply(x, pad, integer(m), m)
  do.call("order", unname(as.list(as.data.frame(t(tmp)))))
}
