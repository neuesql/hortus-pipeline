cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME hortus_pipeline)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/hortus_pipeline_extension.cpp
    src/catalog/materialized_view_catalog.cpp
    src/parser/pipeline_parser.cpp
    src/executor/expectation_checker.cpp
    src/executor/dag_resolver.cpp
    src/executor/materializer.cpp
    src/functions/create_materialized_view.cpp
    src/functions/refresh_materialized_view.cpp
    src/functions/alter_materialized_view.cpp
    src/functions/drop_materialized_view.cpp
    src/functions/pipeline_status.cpp
    src/functions/pipeline_schedules.cpp
    src/scheduler/scheduler.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
