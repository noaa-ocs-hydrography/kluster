# standard modules
import sys
import argparse


# custom modules
from HSTB.kluster.gui import kluster_main
from HSTB.kluster.fqpr_convenience import *


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


class SmartFormatter(argparse.HelpFormatter):

    def _split_lines(self, text, width):
        if text.startswith('R|'):
            return text[2:].splitlines()
        # this is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)


if __name__ == "__main__":  # run from command line
    parser = argparse.ArgumentParser(formatter_class=SmartFormatter)
    subparsers = parser.add_subparsers(help='Available processing commands within Kluster currently', dest='kluster_function')

    allprochelp = 'R|Convert and apply all processing steps to the provided data, combines convert_multibeam, import_navigation and process_multibeam\n'
    allprochelp += 'example (relying on default arguments): all_processing -f fileone.all\n'
    allprochelp += 'example (custom datum/output folder): all_processing -f fileone.all, filetwo.all -datum WGS84 -o new/output/folder'
    allproc = subparsers.add_parser('all_processing', help=allprochelp)
    allproc.add_argument('-f', '--files', nargs='+', required=True,
                         help='Either a single supported multibeam file, multiple supported multibeam files, or a path to a directory of multibeam files')
    allproc.add_argument('-n', '--navigation', nargs='+', required=False,
                         help='list of postprocessed navigation (POSPac) file paths.  If provided, expects either a log file or weekstart_year/weekstart_week/override_datum arguments')
    allproc.add_argument('-err', '--error_files', nargs='+', required=False,
                         help='optional, for use with --navigation, list of postprocessed error (POSPac) file paths.  If provided, must be same number as nav files')
    allproc.add_argument('-log', '--export_log', nargs='+', required=False,
                         help='for use with --navigation, list of export log (POSPac) file paths.  If provided, must be same number as nav files')
    allproc.add_argument('-w_year', '--weekstart_year', required=False,
                         help='for use with --navigation, if export_log is not provided, this must be the year of the sbet')
    allproc.add_argument('-w_week', '--weekstart_week', required=False,
                         help='for use with --navigation, if export_log is not provided, this must be the GPS Week number of the sbet')
    allproc.add_argument('-ng', '--max_navigation_gap', required=False, type=float, nargs='?', const=1.0, default=1.0,
                         help='for use with --navigation, maximum allowable time gap in the sbet in seconds, default is 1 second')
    allproc.add_argument('-o', '--output_folder', required=False,
                         help='full file path to a directory you want to contain all the zarr folders.  Will create this folder if it does not exist.')
    allproc.add_argument('-coord', '--coordinate_system', required=False, nargs='?', const='NAD83', default='NAD83',
                         help='a valid datum identifier that pyproj CRS will accept (WGS84, NAD83, etc.), default is NAD83')
    allproc.add_argument('-vert', '--vertical_reference', required=False, nargs='?', const='waterline', default='waterline',
                         help='the vertical reference point, one of "ellipse", "waterline", default is waterline')
    allproc.add_argument('-cast', '--cast_profiles', nargs='+', required=False,
                         help='either a list of files to include or the path to a directory containing files.  These are in addition to the casts in the ping dataset.')
    allproc.add_argument('-prog', '--show_progress', type=str2bool, required=False, nargs='?', const=True, default=True,
                         help='If true, uses dask.distributed.progress to show progress bar, default is True')
    allproc.add_argument('-parallel', '--parallel_write', type=str2bool, required=False, nargs='?', const=True, default=True,
                         help='If true, writes to disk in parallel, turn this off to troubleshoot PermissionErrors, default is True')

    args = parser.parse_args()
    if not args.kluster_function:
        kluster_main.main()
    else:
        funcname = args.kluster_function
        if funcname == 'all_processing':
            perform_all_processing(args.files, navfiles=args.navigation, outfold=args.output_folder, coord_system=args.coordinate_system,
                                   vert_ref=args.vertical_reference, add_cast_files=args.cast_profiles, show_progress=args.show_progress,
                                   parallel_write=args.parallel_write, error_files=args.error_files, logfiles=args.export_log,
                                   weekstart_year=args.weekstart_year, weekstart_week=args.weekstart_week, override_datum=args.coordinate_system,
                                   max_gap_length=args.max_navigation_gap)
        else:
            print('Unknown function {}'.format(funcname))
