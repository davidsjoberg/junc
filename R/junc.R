# TODO:
# Solve dependencies
# Add handling of column name duplicates between frames (i.e. implement suffix argument)
# Implement keep argument
# Write documentation
# Write tests

base_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE, type)  {

  # Add row identifiers to x and y
  x <- x %>%
    mutate(tmp_row_number_x = row_number())
  y <- y %>%
    mutate(tmp_row_number_y = row_number())

  # Categorize tibbles according to their relative size (for efficiency)
  x_longer_than_y <- nrow(x) > nrow(y)

  if(x_longer_than_y) {
    short <- y
    long <- x
  } else {
    short <- x
    long <- y
  }

  # Format and prepare join conditions
  join_conditions <- enquos(...) %>%
    as.character()

  # Add RHS equal condition for common variable names
  join_conditions <- join_conditions %>%
    str_split(" ") %>%
    sapply(function(long) ifelse(length(long) == 1, paste(long, "==", long %>% str_remove("~")), paste0(long, collapse = " ")))

  # Prefix RHS of condition for map compatability
  join_conditions <- join_conditions %>%
    as.character() %>%
    str_replace(" ([A-Za-z]+.*?$)", " this_row$\\1")

  # Remove '~' and collapse to combined filter statement
  join_conditions <- join_conditions %>%
    str_remove_all("~") %>%
    paste0(collapse = " & ")

  # Execute the join
  result <- map_dfr(1:nrow(short), ~ {
    this_row <- slice(short, .x)

    long %>%
      filter(eval(rlang::parse_expr(join_conditions))) %>%
      bind_cols(this_row %>%
                  select(setdiff(colnames(this_row), colnames(long))))
  })

  # Look for multiple potential joins
  if(unique) {

    if(x_longer_than_y) {
      duplicates <- result %>%
      filter(duplicated(tmp_row_number_x))
    } else {
      duplicates <- result %>%
        filter(duplicated(tmp_row_number_y))
    }

    duplicates <- nrow(duplicates)

    if(duplicates) {
      stop(str_glue("Multiple potential matches, set unique = FALSE if you want to allow this behaviour"))
    }
  }

  # Add/Remove non-matched rows depending on join type
  if(type %in% c("left", "full")) {
    result <- result %>%
      bind_rows(x %>%
                  filter(!(tmp_row_number_x %in% result$tmp_row_number_x)))
  }

  if(type %in% c("right", "full")) {
    result <- result %>%
      bind_rows(x %>%
                  filter(!(tmp_row_number_y %in% result$tmp_row_number_y)))
  }

  if(type == "semi") {
    result <- x %>%
      filter(tmp_row_number_x %in% result$tmp_row_number_x)
  }

  if(type == "anti") {
    result <- x %>%
      filter(!(tmp_row_number_x %in% result$tmp_row_number_x))
  }

  # Arrange rows, rerrange columns and remove temporary row numbers
  result <- result %>%
    arrange(tmp_row_number_x) %>%
    select(c(colnames(x), colnames(y)) %>% unique(),
           -c("tmp_row_number_x", "tmp_row_number_y"))

  return(result)
}

left_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE) {
  base_junc(x, y, ..., suffix = suffix, keep = keep, unique = unique, type = "left")
}

right_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE) {
  base_junc(x, y, ..., suffix = suffix, keep = keep, unique = unique, type = "right")
}

inner_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE) {
  base_junc(x, y, ..., suffix = suffix, keep = keep, unique = unique, type = "inner")
}

full_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE) {
  base_junc(x, y, ..., suffix = suffix, keep = keep, unique = unique, type = "full")
}

semi_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE) {
  base_junc(x, y, ..., suffix = suffix, keep = keep, unique = unique, type = "semi")
}

anti_junc <- function (x, y, ..., suffix = c("", ".y"), keep = FALSE, unique = TRUE) {
  base_junc(x, y, ..., suffix = suffix, keep = keep, unique = unique, type = "anti")
}

