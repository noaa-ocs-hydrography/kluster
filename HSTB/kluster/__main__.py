# standard modules
import sys
import argparse


# custom modules
try:  # this will work unless we are trying to run in headless mode
    from HSTB.kluster.gui import kluster_main
    gui_disabled = False
except ImportError:  # headless mode, importerror stems from kluster_main specifying pyqt, gui will be disabled, matplotlib will be using the 'headless' backend
    gui_disabled = True
from HSTB.kluster.fqpr_convenience import *
from HSTB.kluster.fqpr_intelligence import intel_process, intel_process_service
from HSTB.kluster import kluster_variables


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
    allproc.add_argument('-indatum', '--input_datum', required=False,
                         help='the basic input datum of the converted multibeam data, defaults to what is encoded in the multibeam data')
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
    allproc.add_argument('-coord', '--coordinate_system', required=False, nargs='?', const=kluster_variables.default_coordinate_system, default=kluster_variables.default_coordinate_system,
                         help='a valid datum identifier that pyproj CRS will accept {}, default is {}'.format(kluster_variables.coordinate_systems, kluster_variables.default_coordinate_system))
    allproc.add_argument('-vert', '--vertical_reference', required=False, nargs='?', const='waterline', default='waterline',
                         help='the vertical reference point, one of {}, default is waterline'.format(kluster_variables.vertical_references))
    allproc.add_argument('-cast', '--cast_profiles', nargs='+', required=False,
                         help='either a list of files to include or the path to a directory containing files.  These are in addition to the casts in the ping dataset.')
    allproc.add_argument('-prog', '--show_progress', type=str2bool, required=False, nargs='?', const=True, default=True,
                         help='If true, uses dask.distributed.progress to show progress bar, default is True')
    allproc.add_argument('-parallel', '--parallel_write', type=str2bool, required=False, nargs='?', const=True, default=True,
                         help='If true, writes to disk in parallel, turn this off to troubleshoot PermissionErrors, default is True')

    intelprochelp = 'R|Use Kluster intelligence module to organize and process all input files.  Files can be a list of files, a single '
    intelprochelp += 'file, or a directory full of files.  Files can be multibeam files, .svp sound velocity profile files, SBET and '
    intelprochelp += 'SMRMSG files, etc.  The Intel module will organize and process each in order of priority.\n'
    intelprochelp += 'example (relying on default arguments): intel_processing -f data/directory/path\n'
    intelprochelp += 'example (custom datum/output folder): intel_processing -f fileone.all, filetwo.all -datum WGS84 -o new/output/folder'
    intelproc = subparsers.add_parser('intel_processing', help=intelprochelp)
    intelproc.add_argument('-f', '--files', nargs='+', required=True,
                           help='Either a single supported file, multiple supported files, or a path to a directory of files')
    intelproc.add_argument('-o', '--output_folder', required=False,
                           help='full file path to a directory you want to contain all the processed data.  Will create this folder if it does not exist.')
    intelproc.add_argument('-coord', '--coordinate_system', required=False, nargs='?', const=kluster_variables.default_coordinate_system, default=kluster_variables.default_coordinate_system,
                           help='a valid datum identifier that pyproj CRS will accept {}, default is {}'.format(kluster_variables.coordinate_systems, kluster_variables.default_coordinate_system))
    intelproc.add_argument('-vert', '--vertical_reference', required=False, nargs='?', const='waterline',
                           default='waterline',
                           help='the vertical reference point, one of {}, default is waterline'.format(kluster_variables.vertical_references))
    intelproc.add_argument('-parallel', '--parallel_write', type=str2bool, required=False, nargs='?', const=True,
                           default=True,
                           help='If true, writes to disk in parallel, turn this off to troubleshoot PermissionErrors, default is True')
    intelproc.add_argument('-fc', '--force_coordinate_system', type=str2bool, required=False, nargs='?', const=True,
                           default=True,
                           help='If true, will force all converted data to have the same coordinate system')
    intelproc.add_argument('-p', '--process_mode', required=False, nargs='?', const='normal',
                           default='normal',
                           help='One of the following process modes: normal=generate the next processing action using the current_processing_status attribute as normal, convert_only=only convert incoming data, return no processing actions, concatenate=process line by line if there is no processed data for that line')

    intelservicehelp = 'R|Use Kluster intelligence module to start a new folder monitoring session and process all new files that show '
    intelservicehelp += 'up in the directory.  Files can be multibeam files, .svp sound velocity profile files, SBET and '
    intelservicehelp += 'SMRMSG files, etc.  The Intel module will organize and process each in order of priority.\n'
    intelservicehelp += 'example (relying on default arguments): intel_service -f data/directory/path\n'
    intelservice = subparsers.add_parser('intel_service', help=intelservicehelp)
    intelservice.add_argument('-f', '--folder', nargs='+', required=True,
                              help='Either a single supported file, multiple supported files, or a path to a directory of files')
    intelservice.add_argument('-o', '--output_folder', required=False,
                              help='full file path to a directory you want to contain all the processed data.  Will create this folder if it does not exist.')
    intelservice.add_argument('-coord', '--coordinate_system', required=False, nargs='?', const=kluster_variables.default_coordinate_system, default=kluster_variables.default_coordinate_system,
                              help='a valid datum identifier that pyproj CRS will accept {}, default is {}'.format(kluster_variables.coordinate_systems, kluster_variables.default_coordinate_system))
    intelservice.add_argument('-vert', '--vertical_reference', required=False, nargs='?', const='waterline',
                              default='waterline',
                              help='the vertical reference point, one of {}, default is waterline'.format(kluster_variables.vertical_references))
    intelservice.add_argument('-parallel', '--parallel_write', type=str2bool, required=False, nargs='?', const=True,
                              default=True,
                              help='If true, writes to disk in parallel, turn this off to troubleshoot PermissionErrors, default is True')
    intelservice.add_argument('-fc', '--force_coordinate_system', type=str2bool, required=False, nargs='?', const=True,
                              default=True,
                              help='If true, will force all converted data to have the same coordinate system')
    intelservice.add_argument('-p', '--process_mode', required=False, nargs='?', const='normal',
                              default='normal',
                              help='One of the following process modes: normal=generate the next processing action using the current_processing_status attribute as normal, convert_only=only convert incoming data, return no processing actions, concatenate=process line by line if there is no processed data for that line')

    converthelp = 'R|Convert multibeam from raw files to xarray datasets within the kluster data structure\n'
    converthelp += 'example (relying on default arguments): convert -f fileone.all\n'
    converthelp += 'example (custom output folder): convert -f fileone.all, filetwo.all -o new/output/folder'
    convertproc = subparsers.add_parser('convert', help=converthelp)
    convertproc.add_argument('-f', '--files', nargs='+', required=True,
                             help='Either a single supported multibeam file, multiple supported multibeam files, or a path to a directory of multibeam files')
    convertproc.add_argument('-indatum', '--input_datum', required=False,
                             help='the basic input datum of the converted multibeam data, defaults to what is encoded in the multibeam data')
    convertproc.add_argument('-o', '--output_folder', required=False,
                             help='full file path to a directory you want to contain all the data folders.  Will create this folder if it does not exist.')
    convertproc.add_argument('-prog', '--show_progress', type=str2bool, required=False, nargs='?', const=True, default=True,
                             help='If true, uses dask.distributed.progress to show progress bar, default is True')
    convertproc.add_argument('-parallel', '--parallel_write', type=str2bool, required=False, nargs='?', const=True, default=True,
                             help='If true, writes to disk in parallel, turn this off to troubleshoot PermissionErrors, default is True')

    procnavhelp = 'R|Import processed navigation (POSPac SBET) into converted kluster dataset (run convert first)\n'
    procnavhelp += 'example (with export log): import_processed_nav "C:/data_dir/em2045_20098_07_11_2018" -nf sbet.out -ef smrmsg.out -lf export.log\n'
    procnavhelp += 'example (without log): import_processed_nav "C:/data_dir/em2045_20098_07_11_2018" -nf sbet.out -ef smrmsg.out -yr 2021 -wk 7 -d WGS84'
    procnavproc = subparsers.add_parser('import_processed_nav', help=procnavhelp)
    procnavproc.add_argument('converted_data_folder', help='Path to converted kluster dataset')
    procnavproc.add_argument('-nf', '--navfiles', nargs='+', required=True, help='list of postprocessed navigation file paths (sbet) (.out, .sbet)')
    procnavproc.add_argument('-ef', '--errorfiles', nargs='+', required=False, help='list of postprocessed error file paths (smrmsg) (.out, .sbet)')
    procnavproc.add_argument('-lf', '--logfiles', nargs='+', required=False, help='list of export log file paths (export.log)')
    procnavproc.add_argument('-yr', '--gps_year', type=int, required=False,
                             help='If export log is not provided, this is the year of the navigation file(s) provided')
    procnavproc.add_argument('-wk', '--gps_week', type=int, required=False,
                             help='If export log is not provided, this is the gps week number of the navigation file(s) provided')
    procnavproc.add_argument('-d', '--datum', required=False,
                             help='If export log is not provided, this is the datum identifier of the imported navigation data (WGS84, NAD83)')
    procnavproc.add_argument('-gl', '--max_allowable_gap', type=float, required=False, nargs='?', const=1.0, default=1.0,
                             help='Maximum allowable gap in comparing the imported navigation with the existing raw navigation, default is 1.0 seconds')

    rawnavhelp = 'R|Overwrite raw navigation with POSMV data (POSMV .000) into converted kluster dataset (run convert first)\n'
    rawnavhelp += 'example: overwrite_raw_nav "C:/data_dir/em2045_20098_07_11_2018" -nf posmv.000 posmv.001 -yr 2021 -wk 7'
    rawnavproc = subparsers.add_parser('overwrite_raw_nav', help=rawnavhelp)
    rawnavproc.add_argument('converted_data_folder', help='Path to converted kluster dataset')
    rawnavproc.add_argument('-nf', '--navfiles', nargs='+', required=True, help='list of raw navigation file paths (POSMV) (.000)')
    rawnavproc.add_argument('-yr', '--gps_year', type=int, required=True,
                            help='The year of the navigation file(s) provided')
    rawnavproc.add_argument('-wk', '--gps_week', type=int, required=True,
                            help='The gps week number of the navigation file(s) provided')
    rawnavproc.add_argument('-gl', '--max_allowable_gap', type=float, required=False, nargs='?', const=1.0, default=1.0,
                            help='Maximum allowable gap in comparing the imported navigation with the existing raw navigation, default is 1.0 seconds')

    svphelp = 'R|Import new sound velocity profiles from .svp files into converted kluster dataset (run convert first)\n'
    svphelp += 'example: import_sound_velocity "C:/data_dir/em2045_20098_07_11_2018" -nf posmv.000 posmv.001 -yr 2021 -wk 7'
    svpproc = subparsers.add_parser('import_sound_velocity', help=svphelp)
    svpproc.add_argument('converted_data_folder', help='Path to converted kluster dataset')
    svpproc.add_argument('-sf', '--svfiles', nargs='+', required=True, help='list of processed sound velocity file paths (Caris SVP) (.svp)')

    processhelp = 'R|Process converted Kluster multibeam datasets with the options provided\n'
    processhelp += 'example, NAD83/UTM/waterline, all processes: process_multibeam "C:/data_dir/em2045_20098_07_11_2018"\n'
    processhelp += 'example, NAD83/UTM/ellipse, all processes: process_multibeam "C:/data_dir/em2045_20098_07_11_2018" -vert ellipse\n'
    processhelp += 'example, WGS84, just georeferencing: process_multibeam "C:/data_dir/em2045_20098_07_11_2018" -or False -bpv False -sv False -co WGS84'
    processproc = subparsers.add_parser('process_multibeam', help=processhelp)
    processproc.add_argument('converted_data_folder', help='Path to converted kluster dataset')
    processproc.add_argument('-or', '--run_orientation', type=str2bool, required=False, nargs='?', const=True,
                             default=True, help='If true, runs the Orientation process, the first step to processing in Kluster')
    processproc.add_argument('-bpv', '--run_beam_vector', type=str2bool, required=False, nargs='?', const=True,
                             default=True, help='If true, builds the corrected beam vectors, the second step to processing in Kluster')
    processproc.add_argument('-sv', '--run_sv_correct', type=str2bool, required=False, nargs='?', const=True,
                             default=True, help='If true, sound velocity corrects the beams, the third step to processing in Kluster')
    processproc.add_argument('-geo', '--run_georeference', type=str2bool, required=False, nargs='?', const=True,
                             default=True, help='If true, georeferences the sound velocity corrected beams, the fourth step to processing in Kluster')
    processproc.add_argument('-tpu', '--run_tpu', type=str2bool, required=False, nargs='?', const=True, default=True,
                             help='If true, calculates the total propagated uncertainty, the fifth step to processing in Kluster')
    processproc.add_argument('-ep', '--use_epsg', type=str2bool, required=False, nargs='?', const=True,
                             default=False, help='If true, will use the epsg code provided to build the coordinate system')
    processproc.add_argument('-co', '--use_coord', type=str2bool, required=False, nargs='?', const=True,
                             default=True, help='If true, will use the coordinate system identifier provided to build the coordinate system')
    processproc.add_argument('-epsg', '--epsg_code', type=int, required=False,
                             help='If -ep is True, will use this identifier to build the coordinate system (ex: 26917)')
    processproc.add_argument('-coord', '--coordinate_identifier', type=str, required=False, nargs='?', const=kluster_variables.default_coordinate_system, default=kluster_variables.default_coordinate_system,
                             help='If -co is True, will use this identifier to build the coordinate system, automatically picking a UTM zone, default is {}'.format(kluster_variables.default_coordinate_system))
    processproc.add_argument('-vert', '--vertical_identifier', type=str, required=False, nargs='?', const='waterline', default='waterline',
                             help='Will use this identifier to build the vertical reference, one of {}, default is waterline'.format(kluster_variables.vertical_references))

    gridhelp = 'R|Generate a new grid from the processed Kluster multibeam datasets provided\n'
    gridhelp += 'example: new_surface -df "C:/data_dir/em2045_20098_07_11_2018" -res 8'
    gridproc = subparsers.add_parser('new_surface', help=gridhelp)
    gridproc.add_argument('-df', '--dataset_folders', nargs='+', required=True, help='list of Kluster processed folders')
    gridproc.add_argument('-res', '--resolution', type=int, required=False, nargs='?', const=8, default=8,
                          help='Resolution of the gridded data, in meters')
    gridproc.add_argument('-o', '--output_folder', type=str, required=False,
                          help='If provided, overrides the default destination with this one')

    validhelp = 'R|Validate the Kluster svcorrected answer with the Kongsberg svcorrected answer in the provided file\n'
    validhelp += 'example: validate testfile.all -m first'
    validproc = subparsers.add_parser('validate', help=validhelp)
    validproc.add_argument('multibeam_file', help='path to an existing multibeam file (*.all, *.kmall)')
    validproc.add_argument('-m', '--analysis_mode', type=str, required=False, nargs='?', const='even', default='even',
                           help='select pings by this mode, even = evenly distributed, random = randomly selected, first = first pings found')
    validproc.add_argument('-n', '--number_of_pings', type=int, required=False, nargs='?', const=10, default=10,
                           help='number of pings to compare, default is 10')

    args = parser.parse_args()
    if not args.kluster_function:
        if gui_disabled:
            print('Unable to start gui - main import failed')
        else:
            kluster_main.main()
    else:
        funcname = args.kluster_function
        reloaded_data = None
        if funcname in ['import_processed_nav', 'overwrite_raw_nav', 'import_sound_velocity', 'process_multibeam']:
            reloaded_data = reload_data(args.converted_data_folder)
            if not reloaded_data:
                print('{}: unable to reload from {}, not a valid converted kluster dataset'.format(funcname, args.converted_data_folder))
                sys.exit()

        if funcname == 'all_processing':
            perform_all_processing(args.files, navfiles=args.navigation, input_datum=args.input_datum, outfold=args.output_folder, coord_system=args.coordinate_system,
                                   vert_ref=args.vertical_reference, add_cast_files=args.cast_profiles, show_progress=args.show_progress,
                                   parallel_write=args.parallel_write, error_files=args.error_files, logfiles=args.export_log,
                                   weekstart_year=args.weekstart_year, weekstart_week=args.weekstart_week, override_datum=args.coordinate_system,
                                   max_gap_length=args.max_navigation_gap)
        elif funcname == 'intel_processing':
            intel_process(args.files, outfold=args.output_folder, coord_system=args.coordinate_system, vert_ref=args.vertical_reference,
                          parallel_write=args.parallel_write, force_coordinate_system=args.force_coordinate_system, process_mode=args.process_mode)
        elif funcname == 'intel_service':
            intel_process_service(args.folder, outfold=args.output_folder, coord_system=args.coordinate_system, vert_ref=args.vertical_reference,
                                  parallel_write=args.parallel_write, force_coordinate_system=args.force_coordinate_system, process_mode=args.process_mode)
        elif funcname == 'convert':
            convert_multibeam(args.files, input_datum=args.input_datum, outfold=args.output_folder, show_progress=args.show_progress,
                              parallel_write=args.parallel_write)
        elif funcname == 'import_processed_nav':
            import_processed_navigation(reloaded_data, navfiles=args.navfiles, errorfiles=args.errorfiles, logfiles=args.logfiles,
                                        weekstart_year=args.gps_year, weekstart_week=args.gps_week, override_datum=args.datum,
                                        max_gap_length=args.max_allowable_gap, overwrite=True)
        elif funcname == 'overwrite_raw_nav':
            overwrite_raw_navigation(reloaded_data, navfiles=args.navfiles, weekstart_year=args.gps_year,
                                     weekstart_week=args.gps_week, overwrite=True)
        elif funcname == 'import_sound_velocity':
            import_sound_velocity(reloaded_data, sv_files=args.svfiles)
        elif funcname == 'process_multibeam':
            process_multibeam(reloaded_data, run_orientation=args.run_orientation, run_beam_vec=args.run_beam_vector, run_svcorr=args.run_sv_correct,
                              run_georef=args.run_georeference, run_tpu=args.run_tpu, use_epsg=args.use_epsg, use_coord=args.use_coord, epsg=args.epsg_code,
                              coord_system=args.coordinate_identifier, vert_ref=args.vertical_identifier)
        elif funcname == 'new_surface':
            reloaded_data = []
            for conv in args.dataset_folders:
                dat = reload_data(conv)
                if not dat:
                    print('{}: unable to reload from {}, not a valid converted kluster dataset'.format(funcname, conv))
                    sys.exit()
                reloaded_data.append(dat)
            generate_new_surface(reloaded_data, resolution=args.resolution, output_path=args.output_folder)
        elif funcname == 'validate':
            output_file = os.path.join(os.path.split(args.multibeam_file)[0], 'validation_export.tif')
            validation_against_xyz88(args.multibeam_file, analysis_mode=args.analysis_mode, numplots=args.number_of_pings,
                                     export=output_file)
        else:
            print('Unknown function {}'.format(funcname))
