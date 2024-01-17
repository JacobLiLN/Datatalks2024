import sys
import pandas as pd

print(sys.argv)

# sys.argv[0] is the name of the file
day = sys.argv[1]

# some fancy stuff with pandas

print('job finished successfully for day = {}'.format(day))