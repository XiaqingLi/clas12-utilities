
null_field = '-'
json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'
job_states = {0:'U', 1:'I', 2:'R', 3:'X', 4:'C', 5:'H', 6:'E'}
job_counts = {'done':0, 'run':0, 'idle':0, 'held':0, 'other':0, 'total':0}
exit_codes = { 202:'cvmfs', 203:'generator', 211:'ls', 204:'gemc', 0:'success/unknown',
               205:'evio2hipo', 207:'recon-util', 208:'hipo-utils', 212:'xrootd'}
cvmfs_error_strings = [ 'Loaded environment state is inconsistent',
  'Command not found','Unable to access the Singularity image','CVMFS ERROR']
#  'No such file or directory', 'Transport endpoint is not connected',
