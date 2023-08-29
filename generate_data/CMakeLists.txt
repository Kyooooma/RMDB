cmake_minimum_required(VERSION 3.25)
project(generate_data)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(generate_data
        clock.cpp
        clock.h
        config.h
        generate_data_file.cpp
        random.cpp
        random.h
        table.h)
