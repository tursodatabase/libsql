cmake_minimum_required(VERSION 3.24.0)
project(test_sqlite3mc C)

find_package(sqlite3mc REQUIRED)

add_executable(${PROJECT_NAME}
  main.c
)

target_link_libraries(${PROJECT_NAME}
  sqlite3mc::sqlite3mc
)