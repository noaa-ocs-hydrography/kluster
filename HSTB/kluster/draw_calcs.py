import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import LineString
from shapely.affinity import rotate


# - X = + Forward, Y = + Starboard, Z = + Down
# - roll = + Port Up, pitch = + Bow Up, gyro = + Clockwise

def run_temp():
    v, ref = construct_acrosstrack_vessel(roll=-2.2)
    construct_acrosstrack_beam(v, ref, beam_pointing_angle=74.58)
    v, ref = construct_acrosstrack_vessel(roll=-2.2)
    construct_acrosstrack_beam(v, ref, beam_pointing_angle=-74.58)


def construct_acrosstrack_vessel(roll=10.0, origin=(0, 0), vess_width=.3, vess_height=.2, mount_roll=10, degrees=True):
    if degrees:
        degroll = roll
        radroll = np.deg2rad(roll)
        deg_ma = mount_roll
    else:
        degroll = np.rad2deg(roll)
        radroll = roll
        deg_ma = np.rad2deg(mount_roll)

    f, diagram = plt.subplots(figsize=(9, 7))
    axes = plt.gca()   # Get Current Axes = gca
    axes.set_xlim([-2, 2])
    axes.set_ylim([-2, 1])
    axes.get_xaxis().set_visible(False)
    axes.get_yaxis().set_visible(False)

    # locally level plane
    diagram.plot([-1, origin[0]], [0, origin[1]], 'blue')
    # plane rel roll angle
    end_roll_plane = [-1, np.tan(radroll)]
    diagram.plot([origin[0], end_roll_plane[0]], [origin[1], end_roll_plane[1]], 'orange')
    roll_plane_text = (end_roll_plane[0], end_roll_plane[1] / 2)
    diagram.annotate('roll (attitude)\n = {} deg'.format(round(degroll, 2)), xy=roll_plane_text, xycoords='data',
                     xytext=(-1.7, 0.5), textcoords='data', arrowprops=dict(arrowstyle='->', connectionstyle='arc3'),
                     fontsize='small')

    # vessel top-of-house rel roll angle
    if roll < 0:
        startwidth_factor = vess_width - (vess_width * .5) + np.abs(.05 * np.tan(radroll))
        endwidth_factor = vess_width + (vess_width * .5) + np.abs(.05 * np.tan(radroll))
    else:
        startwidth_factor = vess_width - (vess_width * .5) - np.abs(.05 * np.tan(radroll))
        endwidth_factor = vess_width + (vess_width * .5) - np.abs(.05 * np.tan(radroll))

    top_of_house_st = (end_roll_plane[0] * startwidth_factor, end_roll_plane[1] * startwidth_factor + vess_height)
    top_of_house_end = (end_roll_plane[0] * endwidth_factor, end_roll_plane[1] * endwidth_factor + vess_height)

    diagram.plot([top_of_house_st[0], top_of_house_end[0]], [top_of_house_st[1], top_of_house_end[1]], 'black')

    # sides of house perpendicular to roll plane
    # https://stackoverflow.com/questions/57065080/draw-perpendicular-line-of-fixed-length-at-a-point-of-another-line
    tophouse = LineString([(top_of_house_st[0], top_of_house_st[1]), (top_of_house_end[0], top_of_house_end[1])])
    baseofhouse = tophouse.parallel_offset(vess_height, 'left')
    diagram.plot([top_of_house_st[0], baseofhouse.boundary[0].x],
                 [top_of_house_st[1], baseofhouse.boundary[0].y], 'black')
    diagram.plot([top_of_house_end[0], baseofhouse.boundary[1].x],
                 [top_of_house_end[1], baseofhouse.boundary[1].y], 'black')

    # base of house / deck
    deck_st = (origin[0], origin[1])
    deck_end = (baseofhouse.boundary[1].x + baseofhouse.boundary[1].x * endwidth_factor,
                baseofhouse.boundary[1].y + baseofhouse.boundary[1].y * endwidth_factor)
    diagram.plot([deck_st[0], deck_end[0]], [deck_st[1], deck_end[1]], 'black')

    # trapezoid body
    keel = tophouse.parallel_offset(vess_height * 3, 'left')
    diagram.plot([keel.boundary[0].x, keel.boundary[1].x],
                 [keel.boundary[0].y, keel.boundary[1].y], 'black')
    diagram.plot([deck_end[0], keel.boundary[1].x], [deck_end[1], keel.boundary[1].y], 'black')
    diagram.plot([deck_st[0], keel.boundary[0].x], [deck_st[1], keel.boundary[0].y], 'black')

    # transducer
    trans = keel.parallel_offset(.05, 'left')
    trans_r = rotate(trans, mount_roll)

    transstart = (trans_r.boundary[0].x - ((trans_r.boundary[0].x - trans_r.boundary[1].x) / 4),
                  trans_r.boundary[0].y - ((trans_r.boundary[0].y - trans_r.boundary[1].y) / 4))
    transend = (trans_r.boundary[1].x + ((trans_r.boundary[0].x - trans_r.boundary[1].x) / 4),
                trans_r.boundary[1].y + ((trans_r.boundary[0].y - trans_r.boundary[1].y) / 4))
    diagram.plot([transstart[0], transend[0]], [transstart[1], transend[1]], 'red')
    diagram.plot([keel.boundary[0].x - ((trans.boundary[0].x - trans.boundary[1].x) / 4),
                  keel.boundary[1].x + ((trans.boundary[0].x - trans.boundary[1].x) / 4)],
                 [keel.boundary[0].y - ((trans.boundary[0].y - trans.boundary[1].y) / 4),
                  keel.boundary[1].y + ((trans.boundary[0].y - trans.boundary[1].y) / 4)], 'red')
    diagram.plot([transstart[0], keel.boundary[0].x - ((trans_r.boundary[0].x - trans_r.boundary[1].x) / 4)],
                 [transstart[1], keel.boundary[0].y - ((trans.boundary[0].y - trans.boundary[1].y) / 4)], 'red')
    diagram.plot([transend[0], keel.boundary[1].x + ((trans_r.boundary[0].x - trans_r.boundary[1].x) / 4)],
                 [transend[1], keel.boundary[1].y + ((trans.boundary[0].y - trans.boundary[1].y) / 4)], 'red')

    # get the boresight location, for use with the other functions
    boresight_perpendicular = trans_r.parallel_offset(.5, 'left')
    boresight_loc = (((transend[0] - transstart[0]) / 2) + transstart[0],
                     ((transend[1] - transstart[1]) / 2) + transstart[1])
    boresight_ref = (boresight_loc,
                     (((boresight_perpendicular.boundary[0].x - boresight_perpendicular.boundary[1].x) / 2) +
                      boresight_perpendicular.boundary[1].x, boresight_perpendicular.boundary[1].y))
    diagram.plot([boresight_ref[0][0], boresight_ref[1][0]], [boresight_ref[0][1], boresight_ref[1][1]], 'r--')

    # indicate roll mount offset
    diagram.annotate('Sonar roll\noffset = {} deg'.format(round(deg_ma, 2)),
                     xy=(boresight_loc[0] - .05, boresight_loc[1]),
                     xycoords='data', xytext=(-1.7, -0.2), textcoords='data',
                     arrowprops=dict(arrowstyle='->', connectionstyle='arc3'), fontsize='small')
    plt.show()

    return diagram, boresight_ref


def construct_acrosstrack_beam(diagram, boresight_ref, beam_pointing_angle=74.58, beam_depression_angle=17.615,
                               degrees=True):
    if degrees:
        deg_bpa = beam_pointing_angle
        deg_bda = beam_depression_angle
    else:
        deg_bpa = np.rad2deg(beam_pointing_angle)
        deg_bda = np.rad2deg(beam_depression_angle)

    # plot out the beam pointing angle offset to the beam
    bref = LineString([(boresight_ref[0][0], boresight_ref[0][1]), (boresight_ref[1][0], boresight_ref[1][1])])
    # shapely -> positive angles = clockwise, negative angles = counterclockwise
    #  flip the sign to align with port up +
    bref_to_bpa = rotate(bref, -deg_bpa, origin=(boresight_ref[0][0], boresight_ref[0][1]))
    diagram.arrow(bref_to_bpa.coords.xy[0][0], bref_to_bpa.coords.xy[1][0],
                  2 * (bref_to_bpa.coords.xy[0][1] - bref_to_bpa.coords.xy[0][0]),
                  2 * (bref_to_bpa.coords.xy[1][1] - bref_to_bpa.coords.xy[1][0]), head_width=0.05, head_length=0.05,
                  color='red')

    # annotate in space halfway between
    bref_to_annotation = rotate(bref, -deg_bpa/2, origin=(boresight_ref[0][0], boresight_ref[0][1]))
    diagram.annotate('beam pointing\nangle = {} deg'.format(round(deg_bpa, 2)),
                     xy=(.5 * (bref_to_annotation.coords.xy[0][1] - bref_to_bpa.coords.xy[0][0]) +
                         bref_to_bpa.coords.xy[0][0],
                         .5 * (bref_to_annotation.coords.xy[1][1] - bref_to_bpa.coords.xy[1][0]) +
                         bref_to_bpa.coords.xy[1][0]),
                     xycoords='data', xytext=(-.5, -1.8), textcoords='data', arrowprops=dict(arrowstyle='->',
                     connectionstyle='arc3'), fontsize='small')

    if deg_bpa < 0:
        bda_ref_dir = .5
        bda_rot_dir = -1
        annotate_xloc = .5
    else:
        bda_ref_dir = -1
        bda_rot_dir = 1
        annotate_xloc = -1.8

    # plot out the beam depression angle
    diagram.plot([bda_ref_dir, boresight_ref[0][0]], [boresight_ref[0][1], boresight_ref[0][1]], 'blue')
    bda = LineString([(bda_ref_dir, boresight_ref[0][1]), (boresight_ref[0][0], boresight_ref[0][1])])
    bda_r = rotate(bda, bda_rot_dir * deg_bda / 2, origin=(boresight_ref[0][0], boresight_ref[0][1]))
    diagram.annotate('beam depression\nangle = {} deg'.format(round(deg_bda, 2)),
                     xy=(.5 * (bda_r.coords.xy[0][1] - bda_r.coords.xy[0][0]) + bda_r.coords.xy[0][0],
                         .5 * (bda_r.coords.xy[1][1] - bda_r.coords.xy[1][0]) + bda_r.coords.xy[1][0]),
                     xycoords='data', xytext=(annotate_xloc, -1.8), textcoords='data',
                     arrowprops=dict(arrowstyle='->', connectionstyle='arc3'), fontsize='small')
