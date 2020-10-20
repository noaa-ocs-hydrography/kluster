import os
import openpyxl

from HSTB.kluster.fqpr_convenience import process_and_export_soundings, generate_new_surface, reload_data
from HSTB.kluster.fqpr_surface import BaseSurface

mode_translator = {'vsCW': 'CW_veryshort', 'shCW': 'CW_short', 'meCW': 'CW_medium', 'loCW': 'CW_long',
                   'vlCW': 'CW_verylong', 'elCW': 'CW_extralong', 'shFM': 'FM_short', 'loFM': 'FM_long',
                   '__FM': 'FM', 'FM': 'FM', 'CW': 'CW', 'VS': 'VeryShallow', 'SH': 'Shallow', 'ME': 'Medium',
                   'DE': 'Deep', 'VD': 'VeryDeep', 'ED': 'ExtraDeep'}

def return_files_from_path(pth, file_ext='.all'):
    """
    Input files can be entered into an xarray_conversion.BatchRead instance as either a list, a path to a directory
    of multibeam files or as a path to a single file.  Here we return all the files in each of these scenarios as a list
    for the gui to display or to be analyzed in some other way

    Provide an optional fileext argument if you want to specify files with a different extension

    Parameters
    ----------
    pth: either a list of files, a string path to a directory or a string path to a file
    file_ext: str, file extension of the file(s) you are looking for

    Returns
    -------
    list, list of files found

    """

    if type(pth) == list:
        if len(pth) == 1 and os.path.isdir(pth[0]):  # a list one element long that is a path to a directory
            return [os.path.join(pth[0], p) for p in os.listdir(pth[0]) if os.path.splitext(p)[1] == file_ext]
        else:
            return [p for p in pth if os.path.splitext(p)[1] == file_ext]
    elif os.path.isdir(pth):
        return [os.path.join(pth, p) for p in os.listdir(pth) if os.path.splitext(p)[1] == file_ext]
    elif os.path.isfile(pth):
        if os.path.splitext(pth)[1] == file_ext:
            return [pth]
        else:
            return []
    else:
        return []


def return_directory_from_data(data):
    """
    Given either a path to a zarr store, a path to a directory of .all files, a list of paths to .all files or a path
    to a single .all file.

    Parameters
    ----------
    data: list or str, a path to a zarr store, a path to a directory of .all files, a list of paths to .all files or
          a path to a single .all file.

    Returns
    -------
    output_directory: str, path to output directory

    """
    try:  # they provided a path to a zarr store or a path to a directory of .all files or a single .all file
        output_directory = os.path.dirname(data)
    except TypeError:  # they provided a list of files
        output_directory = os.path.dirname(data[0])
    return output_directory


def return_data(pth, coord_system='NAD83', vert_ref='waterline', require_raw_data=True, autogenerate=True, skip_dask=False):
    """
    Take in a path to a zarr store, a path to a directory of .all files, a list of paths to .all files or a path
    to a single .all file and return a loaded or newly constructed fqpr_generation.Fqpr instance.

    Parameters
    ----------
    pth: list or str, a path to a zarr store, a path to a directory of .all files, a list of paths to .all files or
          a path to a single .all file.
    coord_system: str, one of ['NAD83', 'WGS84']
    vert_ref: str, one of ['waterline', 'ellipse', 'vessel']
    require_raw_data: bool, if True, raise exception if you can't find the raw data
    autogenerate: bool, if True will build a new xyz dataset if a path is passed in
    skip_dask: bool, if True, will not start/find the dask client.  Only use this if you are just reading attribution

    Returns
    -------
    fqpr_generation.Fqpr instance for the given data

    """
    fq = None
    try:
        fq = reload_data(pth, require_raw_data=require_raw_data, skip_dask=skip_dask)
    except (TypeError, ValueError):
        if autogenerate:
            fq = process_and_export_soundings(pth, coord_system=coord_system, vert_ref=vert_ref)
    return fq


def return_surface(ref_surf_pth, vert_ref, resolution, autogenerate=True):
    """
    Take in a path to a zarr store, a path to a directory of .all files, a list of paths to .all files, a path
    to a single .all file or a path to an existing surface.  Return a loaded or newly constructed
    fqpr_surface BaseSurface instance.

    Parameters
    ----------
    ref_surf_pth: list or str, a path to a zarr store, a path to a directory of .all files, a list of paths to .all
                  files, a path to a single .all file or a path to an existing surface.
    vert_ref: str, one of ['waterline', 'ellipse', 'vessel']
    resolution: int, resolution of the grid in meters
    autogenerate: bool, if True will build a new surface if a path is passed in

    Returns
    -------
    fqpr_surface.BaseSurface for the given data

    """
    need_surface = False
    bs = None

    if os.path.isfile(ref_surf_pth):
        if os.path.splitext(ref_surf_pth)[1] not in ['.all', '.kmall']:
            bs = BaseSurface(from_file=ref_surf_pth)
        else:
            need_surface = True
    else:
        need_surface = True

    if need_surface and autogenerate:
        new_surf_path = os.path.join(return_directory_from_data(ref_surf_pth), 'surface_{}m.npy'.format(resolution))
        fq = return_data(ref_surf_pth, vert_ref, autogenerate=True)
        bs = generate_new_surface(fq, resolution, client=fq.client)
        bs.save(new_surf_path)
    return bs


def get_attributes_from_zarr_stores(list_dir_paths, include_mode=True):
    """
    Takes in a list of paths to directories containing fqpr generated zarr stores.  Returns a list where each element
    is a dict of attributes found in each zarr store.  Prefers the attributes from the xyz_dat store, but if it can't
    find it, will read from one of the raw_ping converted zarr stores.  (all attributes across raw_ping data stores
    are identical)

    If include_mode is True, will also read and translate unique modes from each raw_ping and include a unique set of
    those modes in the returned attributes.

    Parameters
    ----------
    list_dir_paths: list, list of strings for paths to each converted folder containing the zarr folders
    include_mode: bool, if True, include mode in the returned attributes

    Returns
    -------
    attrs: list of dicts for each successfully reloaded fqpr object

    """
    attrs = []
    for pth in list_dir_paths:
        fq = reload_data(pth, skip_dask=True)
        if fq is not None:
            newattrs = get_attributes_from_fqpr(fq)
            attrs.append(newattrs)
        else:
            attrs.append([None])
    return attrs


def get_attributes_from_fqpr(fq, include_mode=True):
    """
    Takes in a FQPR instance.  Returns a dict of the attribution in that instance.  Prefers the attributes from the
    xyz_dat store, but if it can't find it, will read from one of the raw_ping converted zarr stores.
    (all attributes across raw_ping data stores are identical)

    If include_mode is True, will also read and translate unique modes from each raw_ping and include a unique set of
    those modes in the returned attributes.

    Parameters
    ----------
    fq: fqpr_generation.FQPR instance
    include_mode: bool, if True, include mode in the returned attributes

    Returns
    -------
    newattrs: dict of attributes in that FQPR instance

    """
    if 'xyz_dat' in fq.__dict__:
        if fq.soundings is not None:
            newattrs = fq.soundings.attrs
        else:
            newattrs = fq.source_dat.raw_ping[0].attrs
    else:
        newattrs = fq.source_dat.raw_ping[0].attrs

    try:
        # update for the min/max nav attributes in raw_nav
        for k, v in fq.source_dat.raw_nav.attrs.items():
            newattrs[k] = v
    except AttributeError:
        print('Unable to read from Navigation')

    if include_mode:
        translated_mode = [mode_translator[a] for a in fq.return_unique_mode()]
        newattrs['mode'] = str(translated_mode)
    return newattrs


def write_all_attributes_to_excel(list_dir_paths, output_excel):
    """
    Using get_attributes_from_zarr_stores, write an excel document, where each row contains the attributes from
    each provided fqpr_generation made fqpr zarr store.

    Parameters
    ----------
    list_dir_paths: list, list of strings for paths to each converted folder containing the zarr folders
    output_excel: str, path to the newly created excel file

    """
    attrs = get_attributes_from_zarr_stores(list_dir_paths)
    headers = list(attrs[0].keys())

    wb = openpyxl.Workbook()
    for name in wb.get_sheet_names():
        if name == 'Sheet':
            temp = wb.get_sheet_by_name('Sheet')
            wb.remove_sheet(temp)
    ws = wb.create_sheet('Kluster Attributes')

    for cnt, h in enumerate(headers):
        ws[chr(ord('@') + cnt + 1) + '1'] = h
    for row_indx, att in enumerate(attrs):
        for cnt, key in enumerate(att):
            ws[chr(ord('@') + cnt + 1) + str(row_indx + 2)] = str(att[key])
    wb.save(output_excel)
