cmake_minimum_required(VERSION 3.11 FATAL_ERROR)

project(seabolt
        DESCRIPTION "Seabolt Demo"
        LANGUAGES C)

add_executable(seabolt-demo main.c)

find_package(seabolt17 REQUIRED)
target_link_libraries(seabolt-demo seabolt17::seabolt-static)