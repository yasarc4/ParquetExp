import pandas as pd
from fastparquet import write
import sys

cols = sys.argv

df = pd.DataFrame([],columns=cols)
write('outfile.parq', df)
