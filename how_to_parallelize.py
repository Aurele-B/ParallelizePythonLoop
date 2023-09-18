"""
This script teaches how to use CPU cores in parallel with python.
It can be used to faster a for loop if each iteration is independent
and if it takes enough time. Use multithreading otherwise.

Aurèle Boussard
"""


from multiprocessing import Queue, Process, Manager
from numpy import (
    savetxt, max, all, any, pi, min, round, mean, diff, square,
    zeros, array, arange, ones_like, isin, repeat, uint8,
    uint64, int64, save, nonzero, max, stack, uint32,
    c_, ceil, empty, float32, expand_dims, cumsum)
from pandas import DataFrame as df
import time
import os
import csv

"""
1. Set variables and function.
ELEMENT_NUMBER is the total number of loop to do.
CORE_NUMBER is the number of core to allocate to this task. Putting maximum may lead to freezing
VARIABLES is a dictionary containing all element any iteration of the function_to_parallelize need to run.
"""

ELEMENT_NUMBER = 200
CORE_NUMBER = os.cpu_count() - 1
VARIABLES = {i: f"{i}" for i in range(ELEMENT_NUMBER)}

"""
2. Import the function to parallelize

"""


def function_to_parallelize(i, variables):
    print(i)
    time.sleep(0.5)
    result = variables[i]
    return {i: result}


def prepare_parallelization(ELEMENT_NUMBER, CORE_NUMBER):
    fair_core_workload = ELEMENT_NUMBER // CORE_NUMBER
    cores_with_1_more = ELEMENT_NUMBER % CORE_NUMBER
    EXTENTS_OF_SUBRANGES = []
    bound = 0
    parallel_organization = [fair_core_workload + 1 for _ in range(cores_with_1_more)] + [fair_core_workload for _ in
                                                                                          range(
                                                                                              CORE_NUMBER - cores_with_1_more)]
    # Emit message to the interface
    print({f"Analysis running on {CORE_NUMBER} CPU cores"})
    for i, extent_size in enumerate(parallel_organization):
        EXTENTS_OF_SUBRANGES.append((bound, bound := bound + extent_size))

    PROCESSES = []
    subtotals = Queue()#Manager().Queue()  #
    return EXTENTS_OF_SUBRANGES, PROCESSES, subtotals


def one_core_workload(lower_bound: int, upper_bound: int, VARIABLES: dict, subtotals: Queue) -> None:
    grouped_results = []
    for i in range(lower_bound, upper_bound):
        results_i = function_to_parallelize(i, VARIABLES)
        grouped_results.append(results_i)
    subtotals.put(grouped_results)


def parallelization(VARIABLES, EXTENTS_OF_SUBRANGES, PROCESSES, subtotals):
    for extent in EXTENTS_OF_SUBRANGES:
        p = Process(target=one_core_workload, args=(extent[0], extent[1], VARIABLES, subtotals))
        p.start()
        PROCESSES.append(p)
    for p in PROCESSES:
        p.join()


    # def save_results(subtotals):
    print(f"Get and group all results")
    final_results = {}
    for i in range(subtotals.qsize()):
        grouped_results = subtotals.get()
        for j, results_i in enumerate(grouped_results):
            current_key = list(results_i.keys())[0]
            final_results[current_key] = results_i[current_key]
    final_results = df.from_dict(final_results, orient='index')
    final_results.to_csv("final_result.csv")


if __name__ == '__main__':
    EXTENTS_OF_SUBRANGES, PROCESSES, subtotals = prepare_parallelization(ELEMENT_NUMBER, CORE_NUMBER)
    parallelization(VARIABLES, EXTENTS_OF_SUBRANGES, PROCESSES, subtotals)
    # save_results(subtotals)

