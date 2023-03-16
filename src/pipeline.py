import concurrent.futures

from enrichment.buo.buo import Buo
from enrichment.esn.esn import Esn
from enrichment.ifa.ifa import Ifa
from preprocessing.preprocessing import PreprocessingJob


class Pipeline:
    def __init__(self) -> None:
        pass

    def __call__(self):
        job1 = PreprocessingJob("config/job1.yaml")
        job1()

        ifa = Ifa()
        buo = Buo()
        esn = Esn()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(ifa.__call__)
            executor.submit(buo.__call__)

        esn()


pipeline = Pipeline()
pipeline()
