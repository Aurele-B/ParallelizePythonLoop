"""
This script teaches how to use CPU cores in parallel with python.
It can be used to faster a for loop if each iteration is independent
and if it takes enough time. Use multithreading otherwise.

Aurèle Boussard
"""


from multiprocessing import Queue, Process
from pandas import DataFrame as df
import time
import os
from pathlib import Path

"""
1. Set variables and function.
ELEMENT_NUMBER is the total number of loop to do.
CORE_NUMBER is the number of core to allocate to this task. Putting maximum may lead to freezing
VARIABLES is a dictionary containing all element any iteration of the function_to_parallelize need to run.
FINAL_RESULT_NAME is a path toward the file to save all result of the iterations
"""

ELEMENT_NUMBER = 200
CORE_NUMBER = os.cpu_count() - 1
VARIABLES = {i: f"{i}" for i in range(ELEMENT_NUMBER)}
FINAL_RESULT_NAME = Path(os.getcwd()) / "final_result.csv"

"""
2. Import the function to parallelize
a. This function must accept the iteration current value "i" and a dictionary containing all necessary variables.
b. This function must produce a resulting dictionary containing the iteration current value "i" and a vector of results.
"""

def function_to_parallelize(i, variables):
    print(i)
    time.sleep(0.5)
    result = variables[i]
    return {i: result}


"""
3. Unchanged functions.
Except if you want to change the nature of the inputs and the outputs of the function_to_parallelize, 
dot not change the following.
"""

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


def parallelization(VARIABLES, FINAL_RESULT_NAME, EXTENTS_OF_SUBRANGES, PROCESSES, subtotals):
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
    final_results.to_csv(FINAL_RESULT_NAME)


"""
4. Run the loop in parallel.
It will write the result at the FINAL_RESULT_NAME location.
The bloc "if __name__ == '__main__':" is necessary.
"""


if __name__ == '__main__':
    EXTENTS_OF_SUBRANGES, PROCESSES, subtotals = prepare_parallelization(ELEMENT_NUMBER, CORE_NUMBER)
    parallelization(VARIABLES, FINAL_RESULT_NAME, EXTENTS_OF_SUBRANGES, PROCESSES, subtotals)

