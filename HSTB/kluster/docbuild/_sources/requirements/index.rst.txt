Requirements
============

As mentioned in the readme, Kluster is built from the ground up in Python, and was developed using Python 3.8.
Kluster has been tested on Windows 10 and Ubuntu 20.04.  See the readme for installation instructions.

Kluster utilizes the following readers.

- kmall - Kongsberg kmall file reader
- prr3 - Reson .s7k file reader
- par3 - Kongsberg .all file reader
- raw - Kongsberg .raw file reader
- sbet - POSPac sbet/rms file reader

Here I will layout the records/setup required for these systems to produce data that is compatible with Kluster.

First though, I'll lay out the basic requirements for any sonar that Kluster can/will handle in the future:

 - Sound Speed Profile (optional) - Kluster prefers a sound speed profile to be delivered in the raw multibeam data.  This allows Kluster to ray-trace any multibeam file regardless of the inclusion of an external profile file.
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

Data89 (Seabed Image - if exists)
 - Time
 - Reflectivity

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

Data89 (Seabed Image - if exists)
 - Time
 - Reflectivity

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

Data89 (Seabed Image - if exists)
 - Time
 - Reflectivity

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
 - Sounding_Reflectivity2_dB

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

Kongsberg .raw
---------------

Kluster supports two separate processing paths for Kongsberg .raw files.  These are as follows:

Each datagram listed will also list the components of that datagram used by Kluster.

NOTE: Kluster will perform the amplitude detection during conversion.  Kluster will also calculate a heave correction.

Path 1 (EK60)
^^^^^^^^^^^^^^^^

CON0 (EK60 Configuration Datagram)
 - Time
 - InstallationSettings

RAW0 (EK60 Sample Datagram)
 - Time
 - Count
 - Offset
 - Power
 - PulseLength
 - SampleInterval
 - TransducerDepth
 - SoundVelocity
 - Frequency
 - Roll
 - Pitch
 - Heading

NME0 (NMEA Text Datagram - prefers GGA)
 - Time
 - Latitude
 - Longitude
 - Altitude (if found in NME0 record)

Path 2 (EK80)
^^^^^^^^^^^^^^^^

NOTE: Saildrone EK80 data may not have NME0 records.  If this is the case, Kluster will look for a csv file with navigation with the extension .gps.csv that is in the same folder as the .raw files.  As long as those files are in the same folder, conversion of the navigation will be done automatically.  The csv file must have columns ('gps_fix', 'gps_date', 'gps_time', 'latitude', 'longitude') as shown in the example below:

GPS_fix,GPS_date,GPS_time,Latitude,Longitude

0,2018-07-01,00:00:00,53.9448,-166.53966079999998


NOTE: Roll Pitch Heading are all set to zero for EK80 workflow, as they are not included in the datagrams


XML0 (Configuration XML Datagram)
 - Time
 - InstallationSettings

XML0 (Channel XML Datagram)
 - Time
 - Frequency
 - PulseDuration
 - SampleInterval

RAW3 (EK80 Sample Datagram)
 - Time
 - Count
 - Offset
 - Power or ComplexSamples, depending on datatype

NME0 (NMEA Text Datagram - prefers GGA)
 - Time
 - Latitude
 - Longitude
 - Altitude (if found in NME0 record)

Reson s7k
-----------

Kluster supports two separate processing paths for Reson .s7k files.  These are as follows (in order of oldest first, to newest last):

Each datagram listed will also list the components of that datagram used by Kluster.

NOTE: will use Data7000 when Data7503 is not available, but Data7503 is preferred for reading BeamSpacingMode and the offsets that are used when 7030 is not available.

Path 1 (Standard)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data1003 (Position)
 - Time
 - LatitudeNorthing
 - LongitudeEasting
 - Height

Data1009 (SoundVelocityProfile - Optional)
 - Time
 - Depth
 - SoundSpeed

Data1012 (Attitude)
 - Time
 - Roll
 - Pitch
 - Heave

Data1013 (Heading)
 - Time
 - Heading

Data7001 (Configuration)
 - Serial Numbers

Data7027 (Raw Detection Data)
 - Time
 - PingNumber
 - TxAngle
 - RxAngle
 - Intensity
 - Uncertainty
 - TravelTime
 - DetectionFlags

Data7030 (Installation Parameters - Optional)
 - Time
 - InstallationSettings

Data7503 or Data7000 (SonarSettings, prefer 7503)
 - Time
 - SoundVelocity
 - TxPulseTypeID
 - TransmitFlags
 - Frequency
 - BeamSpacingMode (if 7503)

Path 2 (Software that generates 1016 record)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data1003 (Position)
 - Time
 - LatitudeNorthing
 - LongitudeEasting
 - Height

Data1009 (SoundVelocityProfile - Optional)
 - Time
 - Depth
 - SoundSpeed

Data1016 (Attitude)
 - Time
 - Roll
 - Pitch
 - Heave
 - Heading

Data7001 (Configuration)
 - Serial Numbers

Data7027 (Raw Detection Data)
 - Time
 - PingNumber
 - TxAngle
 - RxAngle
 - Intensity
 - Uncertainty
 - TravelTime
 - DetectionFlags

Data7030 (Installation Parameters - Optional)
 - Time
 - InstallationSettings

Data7503 or Data7000 (SonarSettings, prefer 7503)
 - Time
 - SoundVelocity
 - TxPulseTypeID
 - TransmitFlags
 - Frequency
 - BeamSpacingMode (if 7503)

.. toctree::
