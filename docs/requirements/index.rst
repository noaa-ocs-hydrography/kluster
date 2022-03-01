Requirements
============

As mentioned in the readme, Kluster is built from the ground up in Python, and was developed using Python 3.8.
Kluster has been tested on Windows 10 and Ubuntu 20.04.  See the readme for installation instructions.

Kluster utilizes the following readers.

- kmall - Kongsberg kmall file reader
- par3 - Kongsberg .all file reader
- sbet - POSPac sbet/rms file reader

Here I will layout the records/setup required for these systems to produce data that is compatible with Kluster.

First though, I'll lay out the basic requirements for any sonar that Kluster can/will handle in the future:

 - Sound Speed Profile - Kluster requires a sound speed profile to be delivered in the raw multibeam data.  This allows Kluster to ray-trace any multibeam file regardless of the inclusion of an external profile file.
 - System Serial Number - Kluster requires that each file have a serial number that uniquely identifies a sonar.  This allows Kluster to catalogue and organize files according to system.
 - Installation Parameters - Kluster requires that the offsets and mounting angles are provided in the file from the sonar reference point, including waterline vertical position.
 - Time Standard - Times must be reported in UTC time within the multibeam file, or provide a way to get to UTC using only the information in the file.
 - Input Position Datum - This is useful for Kluster to understand the source datum of the data, for future vertical and horizontal transformations.

Optionally, the multibeam should provide a ray-traced processed answer, using the included sound speed profile.  I believe this is a great way to ensure that the user is also able to process the multibeam file without any additional files needed.

Kongsberg .all
---------------

Kluster supports three separate processing paths for Kongsberg .all files.  These are as follows (in order of oldest first, to newest last):

Each datagram listed will also list the components of that datagram used by Kluster.

Path 1 (for older sonars)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data102 (Raw range and beam angle (f))
 - Time
 - PingCounter
 - SoundSpeed
 - Ntx
 - SystemSerialNumber
 - TiltAngle
 - Delay
 - CenterFrequency
 - BeamPointingAngle
 - TransmitSectorID
 - DetectionWindowLength
 - QualityFactor
 - TravelTime

Data73 (Installation Parameters)
 - Time
 - InstallationParameterText

Data65 (Attitude)
 - Time
 - Roll
 - Pitch
 - Heave
 - Heading

Data82 (Runtime Parameters)
 - Time
 - Mode
 - ModeTwo
 - YawPitchStabilization
 - RuntimeParameterText

Data85 (Sound Speed Profile)
 - Time
 - DepthProfile
 - SoundspeedProfile

Data80 (Position)
 - Time
 - Latitude
 - Longitude
 - Altitude (decoded from Position Input Datagram)

Path 2 (newer systems without Network Attitude)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data78 (Raw range and angle)
 - Time
 - PingCounter
 - SoundSpeed
 - Ntx
 - SystemSerialNumber
 - TiltAngle
 - Delay
 - Frequency
 - BeamPointingAngle
 - TransmitSectorID
 - DetectionInfo
 - QualityFactor
 - TravelTime

Data73 (Installation Parameters)
 - Time
 - InstallationParameterText

Data65 (Attitude)
 - Time
 - Roll
 - Pitch
 - Heave
 - Heading

Data82 (Runtime Parameters)
 - Time
 - Mode
 - ModeTwo
 - YawPitchStabilization
 - RuntimeParameterText

Data85 (Sound Speed Profile)
 - Time
 - DepthProfile
 - SoundspeedProfile

Data80 (Position)
 - Time
 - Latitude
 - Longitude
 - Altitude (decoded from Position Input Datagram)

Path 3 (newer systems with Network Attitude)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data78 (Raw range and angle)
 - Time
 - PingCounter
 - SoundSpeed
 - Ntx
 - SystemSerialNumber
 - TiltAngle
 - Delay
 - Frequency
 - BeamPointingAngle
 - TransmitSectorID
 - DetectionInfo
 - QualityFactor
 - TravelTime

Data73 (Installation Parameters)
 - Time
 - InstallationParameterText

Data65 (Attitude)
 - Time
 - Roll
 - Pitch
 - Heave
 - Heading

Data82 (Runtime Parameters)
 - Time
 - Mode
 - ModeTwo
 - YawPitchStabilization
 - RuntimeParameterText

Data85 (Sound Speed Profile)
 - Time
 - DepthProfile
 - SoundspeedProfile

Data110 (Network Attitude)
 - Time
 - Latitude
 - Longitude
 - Altitude

Kongsberg .kmall
----------------

There is a single processing path using KMALL files that is supported by Kluster.

Each datagram listed will also list the components of that datagram used by Kluster.

Path 1
^^^^^^^

SKM (Attitude and Attitude Velocity Sensors)
 - Time
 - Roll
 - Pitch
 - Heave
 - Heading

MRZ (Multibeam Raw Range and Depth)
 - Time
 - PingCounter
 - PingInfo_SoundSpeedatTxDepth
 - PingInfo_NumberTxSectors
 - PingInfo_ModeAndStabilization
 - PingInfo_PulseForm
 - PingInfo_DepthMode
 - PingInfo_Latitude
 - PingInfo_Longitude
 - PingInfo_EllipsoidHeightRefPoint
 - TxSectorInfo_ArrayNumber
 - TxSectorInfo_TiltAngleReTx
 - TxSectorInfo_SectorTransmitDelay
 - TxSectorInfo_CenterFrequency
 - Sounding_BeamAngleReRx
 - Sounding_TxSectorNumber
 - Sounding_DetectionType
 - Sounding_DetectionMethod
 - Sounding_QualityFactor
 - Sounding_TwoWayTravelTime

IIP (Installation Parameters)
 - Time
 - InstallationSettings

IOP (Runtime Parameters)
 - Time
 - RuntimeSettings

SVP (Sound Velocity Profile)
 - Time
 - DepthProfile
 - SoundSpeedProfile

.. toctree::
