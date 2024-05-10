/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.cosem.analysis;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.cosem.util.AbstractOptions;
import org.janelia.cosem.util.BlockInformation;
import org.janelia.cosem.util.IOHelper;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.Views;


/**
 * Connected components for an entire n5 volume
 *
 * @author David Ackerman &lt;ackermand@janelia.hhmi.org&gt;
 */
public class SparkGeneralCosemObjectInformation {
	@SuppressWarnings("serial")
	public static class Options extends AbstractOptions implements Serializable {

		@Option(name = "--inputN5Path", required = true, usage = "input N5 path, e.g. /nrs/saalfeld/heinrichl/cell/gt061719/unet/02-070219/hela_cell3_314000.n5")
		private String inputN5Path = null;

		@Option(name = "--outputDirectory", required = false, usage = "output N5 path, e.g. /nrs/flyem/data/tmp/Z0115-22.n5")
		private String outputDirectory = null;

		@Option(name = "--inputN5DatasetName", required = false, usage = "N5 dataset, e.g. /mito")
		private String inputN5DatasetName = null;

		@Option(name = "--inputPairs", required = false, usage = "Pairs that have contact sites, eg. 'a_to_b,c_to_d'")
		private String inputPairs = null;

		@Option(name = "--skipContactSites", required = false, usage = "Skip general information for contact sites")
		private boolean skipContactSites = false;

		@Option(name = "--skipSelfContacts", required = false, usage = "Skip general information for self contact sites")
		private boolean skipSelfContacts = false;

		public Options(final String[] args) {
			final CmdLineParser parser = new CmdLineParser(this);
			try {
				parser.parseArgument(args);
				parsedSuccessfully = true;
			} catch (final CmdLineException e) {
				parser.printUsage(System.err);
			}
		}

		public String getInputN5Path() {
			return inputN5Path;
		}

		public String getInputN5DatasetName() {
			return inputN5DatasetName;
		}

		public String getInputPairs() {
			return inputPairs;
		}

		public String getOutputDirectory() {
			if(outputDirectory == null) {
				outputDirectory = inputN5Path.split(".n5")[0]+"_results";
			}
			return outputDirectory;
		}

		public boolean getSkipContactSites() {
			return skipContactSites;
		}

		public boolean getSkipSelfContacts() {
			return skipSelfContacts;
		}

	}

	public static class InBoundsChecker{
		private long[] offset, overallDimensions;

		public InBoundsChecker(long [] offset, long [] overallDimensions) {
			this.offset = offset;
			this.overallDimensions = overallDimensions;
		}

		public boolean voxelIsInBounds(long [] position) {
			long [] overallPosition = new long [] {position[0]+offset[0],position[1]+offset[1],position[2]+offset[2]};
			if(overallPosition[0]<0 || overallPosition[1]<0 || overallPosition[2]<0 ||
					overallPosition[0]>=overallDimensions[0] || overallPosition[1]>=overallDimensions[1] || overallPosition[2]>=overallDimensions[2]) {
				return false;
			}
			return true;
		}

	}

	/**
	 * Find connected components on a block-by-block basis and write out to
	 * temporary n5.
	 *
	 * Takes as input a threshold intensity, above which voxels are used for
	 * calculating connected components. Parallelization is done using a
	 * blockInformationList.
	 *
	 * @param sc
	 * @param inputN5Path
	 * @param inputN5DatasetName
	 * @param outputN5Path
	 * @param outputN5DatasetName
	 * @param maskN5PathName
	 * @param thresholdIntensity
	 * @param blockInformationList
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends IntegerType<T> & NativeType<T>> void calculateVolumeAreaCount(
			final JavaSparkContext sc, final String inputN5Path, final String[] datasetNames, final String outputDirectory,
			List<BlockInformation> blockInformationList) throws IOException {

		final String inputN5DatasetName, organelle1ContactBoundaryN5Dataset, organelle2ContactBoundaryN5Dataset;
		if(datasetNames.length==1) {
			organelle1ContactBoundaryN5Dataset=null;
			organelle2ContactBoundaryN5Dataset=null;
			inputN5DatasetName = datasetNames[0];
		}
		else {
			organelle1ContactBoundaryN5Dataset = datasetNames[0]+"_contact_boundary_temp_to_delete";
			organelle2ContactBoundaryN5Dataset = datasetNames[1]+"_contact_boundary_temp_to_delete";
			inputN5DatasetName = datasetNames[2];
		}
		final N5Reader n5Reader = new N5FSReader(inputN5Path);
		final DatasetAttributes attributes = n5Reader.getDatasetAttributes(inputN5DatasetName);
		final long[] outputDimensions = attributes.getDimensions();
		double [] pixelResolution = IOHelper.getResolution(n5Reader, inputN5DatasetName);
		int[] datasetOffset = IOHelper.getOffset(n5Reader, inputN5DatasetName);

		// Set up rdd to parallelize over blockInformation list and run RDD, which will
		// return updated block information containing list of components on the edge of
		// the corresponding block
		// Set up reader to get n5 attributes

		final JavaRDD<BlockInformation> rdd = sc.parallelize(blockInformationList);
		JavaRDD<Map<Long,long[]>> javaRDDvolumeAreaCount  = rdd.map(currentBlockInformation -> {
			// Get information for reading in/writing current block
			long[][] gridBlock = currentBlockInformation.gridBlock;
			long[] extendedOffset = gridBlock[0];
			long[] dimension = gridBlock[1].clone(), extendedDimension = gridBlock[1].clone();

			//extend by 1 on each edge
			Arrays.setAll(extendedOffset, i->extendedOffset[i]-1);
			Arrays.setAll(extendedDimension, i->extendedDimension[i]+2);

			// Read in source block
			final N5Reader n5ReaderLocal = new N5FSReader(inputN5Path);
			final RandomAccessibleInterval<T> sourceInterval = Views.offsetInterval(Views.extendZero(
					(RandomAccessibleInterval<T>) N5Utils.open(n5ReaderLocal, inputN5DatasetName)), extendedOffset, extendedDimension);
			final RandomAccess<T> sourceRandomAccess = sourceInterval.randomAccess();

			RandomAccess<T> organelle1ContactBoundaryRandomAccess = null,organelle2ContactBoundaryRandomAccess=null, organelle1RandomAccess = null, organelle2RandomAccess = null;
			if(datasetNames.length>1) {
				organelle1ContactBoundaryRandomAccess = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<T>) N5Utils.open(n5ReaderLocal, organelle1ContactBoundaryN5Dataset)), extendedOffset, extendedDimension).randomAccess();
				organelle2ContactBoundaryRandomAccess = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<T>) N5Utils.open(n5ReaderLocal, organelle2ContactBoundaryN5Dataset)), extendedOffset, extendedDimension).randomAccess();
				organelle1RandomAccess = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<T>) N5Utils.open(n5ReaderLocal, datasetNames[0])), extendedOffset, extendedDimension).randomAccess();
				organelle2RandomAccess = Views.offsetInterval(Views.extendZero(
						(RandomAccessibleInterval<T>) N5Utils.open(n5ReaderLocal, datasetNames[1].split("_pairs")[0])), extendedOffset, extendedDimension).randomAccess();

			}

			Map<Long, long[]> objectIDtoInformationMap = new HashMap<>(); //Volume, Surface Area, com xyz, min xyz, max xyz

			//For surface area
			List<long[]> voxelsToCheck = new ArrayList();
			voxelsToCheck.add(new long[] {-1, 0, 0});
			voxelsToCheck.add(new long[] {1, 0, 0});
			voxelsToCheck.add(new long[] {0, -1, 0});
			voxelsToCheck.add(new long[] {0, 1, 0});
			voxelsToCheck.add(new long[] {0, 0, -1});
			voxelsToCheck.add(new long[] {0, 0, 1});
			InBoundsChecker inBoundsChecker = new InBoundsChecker(extendedOffset, outputDimensions);
			for(long x=1; x<=dimension[0]; x++) {
				for(long y=1; y<=dimension[1]; y++) {
					for(long z=1; z<=dimension[2]; z++) {
						long [] pos = new long[] {x,y,z};
						sourceRandomAccess.setPosition(pos);

						long currentVoxelValue=sourceRandomAccess.get().getIntegerLong();

						if (currentVoxelValue >0  && inBoundsChecker.voxelIsInBounds(pos) ) {

							int surfaceAreaContributionOfVoxelInFaces = getSurfaceAreaContributionOfVoxelInFaces(sourceRandomAccess, inBoundsChecker, voxelsToCheck);

							long[] absolutePosition = {(long) (x+extendedOffset[0]+datasetOffset[0]/pixelResolution[0]),(long) (y+extendedOffset[1]+datasetOffset[1]/pixelResolution[1]),(long) (z+extendedOffset[2]+datasetOffset[2]/pixelResolution[2])};
							long[] organelleIDs = {-1, -1};
							long[] organelleSurfaceAreas = {-1, -1};
							if(datasetNames.length>1) {
								organelle1ContactBoundaryRandomAccess.setPosition(new long[] {x,y,z});
								organelle2ContactBoundaryRandomAccess.setPosition(new long[] {x,y,z});
								organelleIDs[0] = organelle1ContactBoundaryRandomAccess.get().getIntegerLong();
								organelleIDs[1] = organelle2ContactBoundaryRandomAccess.get().getIntegerLong();

								organelle1RandomAccess.setPosition(new long[] {x,y,z});
								organelle2RandomAccess.setPosition(new long[] {x,y,z});
								organelleSurfaceAreas[0] = getSurfaceAreaContributionOfVoxelInFaces(organelle1RandomAccess, inBoundsChecker, voxelsToCheck);
								organelleSurfaceAreas[1] = getSurfaceAreaContributionOfVoxelInFaces(organelle2RandomAccess, inBoundsChecker, voxelsToCheck);
							}
							addNewVoxelToObjectInformation(objectIDtoInformationMap, currentVoxelValue, absolutePosition, surfaceAreaContributionOfVoxelInFaces, organelleIDs, organelleSurfaceAreas);
						}
					}
				}
			}

			return objectIDtoInformationMap;
		});

		Map<Long, long[]> collectedObjectInformation = javaRDDvolumeAreaCount.reduce((a,b) -> {
			combineObjectInformationMaps(a,b);
			return a;
		});

		System.out.println("Total objects: "+collectedObjectInformation.size());
		writeData(collectedObjectInformation, outputDirectory, datasetNames, pixelResolution[0]);//Assuming it is isotropic
	}

	public static final String VOLUME_INDEX = "0";
	public static final String SURFACE_AREA_INDEX = "1";
	public static final String POSITION_X_INDEX = "2";
	public static final String MIN_X_INDEX = "5";
	public static final String MAX_X_INDEX = "8";
	public static final String ORGANELLE_ID_1_INDEX = "11";
	public static final String ORGANELLE_ID_2_INDEX = "12";
	public static final String ORGANELLE_SURFACE_AREA_1_INDEX = "13";
	public static final String ORGANELLE_SURFACE_AREA_2_INDEX = "14";

	public static void addNewVoxelToObjectInformation(Map<Long,long[]> objectIDtoInformationMap, long objectID, long[] position, long surfaceAreaContributionOfVoxelInFaces, long[] organelleIDs, long[] organelleSurfaceAreas) {
		if(!objectIDtoInformationMap.containsKey(objectID)) {
			objectIDtoInformationMap.put(objectID, new long[]{1,surfaceAreaContributionOfVoxelInFaces,position[0],position[1],position[2],position[0],position[1],position[2],position[0],position[1],position[2], organelleIDs[0], organelleIDs[1], organelleSurfaceAreas[0], organelleSurfaceAreas[1]});
		}
		else {
			long[] objectInformation = objectIDtoInformationMap.get(objectID);

			incrementVolume(objectInformation);
			incrementSurfaceArea(objectInformation, surfaceAreaContributionOfVoxelInFaces);
			updateCenterOfMass(objectInformation, position);
			updateMinPosition(objectInformation, position);
			updateMaxPosition(objectInformation, position);
			updateOrganelleIds(objectInformation, organelleIDs);
			updateOrganelleSurfaceAreas(objectInformation, organelleSurfaceAreas);
		}
	}

	private static void incrementVolume(long[] objectInformation) {
		objectInformation[Integer.parseInt(VOLUME_INDEX)] += 1;
	}

	private static void incrementSurfaceArea(long[] objectInformation, long surfaceAreaContributionOfVoxelInFaces) {
		objectInformation[Integer.parseInt(SURFACE_AREA_INDEX)] += surfaceAreaContributionOfVoxelInFaces;
	}

	private static void updateCenterOfMass(long[] objectInformation, long[] position) {
		for(int i=0; i<3; i++) {
			objectInformation[Integer.parseInt(POSITION_X_INDEX) + i] += position[i];
		}
	}

	private static void updateMinPosition(long[] objectInformation, long[] position) {
		for(int i=0; i<3; i++) {
			objectInformation[Integer.parseInt(MIN_X_INDEX) + i] = Math.min(objectInformation[Integer.parseInt(MIN_X_INDEX) + i], position[i]);
		}
	}

	private static void updateMaxPosition(long[] objectInformation, long[] position) {
		for(int i=0; i<3; i++) {
			objectInformation[Integer.parseInt(MAX_X_INDEX) + i] = Math.max(objectInformation[Integer.parseInt(MAX_X_INDEX) + i], position[i]);
		}
	}

	private static void updateOrganelleIds(long[] objectInformation, long[] organelleIDs) {
		objectInformation[Integer.parseInt(ORGANELLE_ID_1_INDEX)] = Math.max(organelleIDs[0],objectInformation[Integer.parseInt(ORGANELLE_ID_1_INDEX)]);
		objectInformation[Integer.parseInt(ORGANELLE_ID_2_INDEX)] = Math.max(organelleIDs[1],objectInformation[Integer.parseInt(ORGANELLE_ID_2_INDEX)]);
	}

	private static void updateOrganelleSurfaceAreas(long[] objectInformation, long[] organelleSurfaceAreas) {
		objectInformation[Integer.parseInt(ORGANELLE_SURFACE_AREA_1_INDEX)] += organelleSurfaceAreas[0];
		objectInformation[Integer.parseInt(ORGANELLE_SURFACE_AREA_2_INDEX)] += organelleSurfaceAreas[1];
	}

	public static Map<Long,long[]> combineObjectInformationMaps(Map<Long,long[]> objectInformationMapA, Map<Long,long[]> objectInformationMapB) {
		for(long objectID : objectInformationMapB.keySet() ) {
			if(objectInformationMapA.containsKey(objectID)) {
				long[] objectInformationA = objectInformationMapA.get(objectID);
				long[] objectInformationB = objectInformationMapB.get(objectID);

				updateVolumeAndSurfaceArea(objectInformationA, objectInformationB);
				updateCenterOfMassA(objectInformationA, objectInformationB);
				updateMinAndMaxPosition(objectInformationA, objectInformationB);
				updateOrganelleIdsA(objectInformationA, objectInformationB);
				updateOrganelleSurfaceAreasA(objectInformationA, objectInformationB);

				objectInformationMapA.put(objectID, objectInformationA);
			}
			else {
				objectInformationMapA.put(objectID, objectInformationMapB.get(objectID));
			}
		}
		return objectInformationMapA;
	}

	private static void updateVolumeAndSurfaceArea(long[] objectInformationA, long[] objectInformationB) {
		for(int i=0; i<2; i++) {
			objectInformationA[i]+=objectInformationB[i]; //Volume, surface area
		}
	}

	private static void updateCenterOfMassA(long[] objectInformationA, long[] objectInformationB) {
		for(int i=0; i<3; i++) {
			objectInformationA[Integer.parseInt(POSITION_X_INDEX) + i]+= objectInformationB[Integer.parseInt(POSITION_X_INDEX) + i]; //com xyz
		}
	}

	private static void updateMinAndMaxPosition(long[] objectInformationA, long[] objectInformationB) {
		for(int i=0; i<3; i++) {
			objectInformationA[Integer.parseInt(MIN_X_INDEX) + i] = Math.min(objectInformationA[Integer.parseInt(MIN_X_INDEX) + i], objectInformationB[Integer.parseInt(MIN_X_INDEX) + i]); //min xyz
			objectInformationA[Integer.parseInt(MAX_X_INDEX) + i] = Math.max(objectInformationA[Integer.parseInt(MAX_X_INDEX) + i], objectInformationB[Integer.parseInt(MAX_X_INDEX) + i]); //max xyz
		}
	}

	private static void updateOrganelleIdsA(long[] objectInformationA, long[] objectInformationB) {
		objectInformationA[Integer.parseInt(ORGANELLE_ID_1_INDEX)] = Math.max(objectInformationA[Integer.parseInt(ORGANELLE_ID_1_INDEX)],objectInformationB[Integer.parseInt(ORGANELLE_ID_1_INDEX)]);
		objectInformationA[Integer.parseInt(ORGANELLE_ID_2_INDEX)] = Math.max(objectInformationA[Integer.parseInt(ORGANELLE_ID_2_INDEX)],objectInformationB[Integer.parseInt(ORGANELLE_ID_2_INDEX)]);
	}

	private static void updateOrganelleSurfaceAreasA(long[] objectInformationA, long[] objectInformationB) {
		objectInformationA[Integer.parseInt(ORGANELLE_SURFACE_AREA_1_INDEX)] += objectInformationB[Integer.parseInt(ORGANELLE_SURFACE_AREA_1_INDEX)];
		objectInformationA[Integer.parseInt(ORGANELLE_SURFACE_AREA_2_INDEX)] += objectInformationB[Integer.parseInt(ORGANELLE_SURFACE_AREA_2_INDEX)];
	}
	public static <T extends IntegerType<T> & NativeType<T>> int getSurfaceAreaContributionOfVoxelInFaces(final RandomAccess<T> sourceRandomAccess, InBoundsChecker inBoundsChecker, List<long[]> voxelsToCheck) {
		long referenceVoxelValue = sourceRandomAccess.get().getIntegerLong();
		final long sourceRandomAccessPosition[] = {sourceRandomAccess.getLongPosition(0), sourceRandomAccess.getLongPosition(1), sourceRandomAccess.getLongPosition(2)};
		int surfaceAreaContributionOfVoxelInFaces = 0;

		if(referenceVoxelValue>0) {
			for(long[] currentVoxel : voxelsToCheck) {
				final long currentPosition[] = {sourceRandomAccessPosition[0]+currentVoxel[0], sourceRandomAccessPosition[1]+currentVoxel[1], sourceRandomAccessPosition[2]+currentVoxel[2]};
				sourceRandomAccess.setPosition(currentPosition);
				if(sourceRandomAccess.get().getIntegerLong() != referenceVoxelValue && inBoundsChecker.voxelIsInBounds(currentPosition)) {
					surfaceAreaContributionOfVoxelInFaces ++;
				}
			}
		}
		return surfaceAreaContributionOfVoxelInFaces;

	}

	public static void writeData(Map<Long,long[]> collectedObjectInformation, String outputDirectory, String [] datasetNames, double pixelDimension) throws IOException {
		if (! new File(outputDirectory).exists()){
			new File(outputDirectory).mkdirs();
		}

		String outputFile, organelle1=null, organelle2=null;
		if(datasetNames.length == 1) {
			outputFile = datasetNames[0];
		}
		else {
			organelle1 = datasetNames[0];
			organelle2 = datasetNames[1];
			outputFile = datasetNames[2];
		}
		FileWriter csvWriter = new FileWriter(outputDirectory+"/"+outputFile+".csv");
		if(datasetNames.length == 1) {
			csvWriter.append("Object ID,Volume (nm^3),Surface Area (nm^2),COM X (nm),COM Y (nm),COM Z (nm),MIN X (nm),MIN Y (nm),MIN Z (nm),MAX X (nm),MAX Y (nm),MAX Z (nm),,Total Objects\n");
		}
		else {
			csvWriter.append("Object ID,Volume (nm^3),Surface Area (nm^2),COM X (nm),COM Y (nm),COM Z (nm),MIN X (nm),MIN Y (nm),MIN Z (nm),MAX X (nm),MAX Y (nm),MAX Z (nm),"+organelle1+" ID,"+organelle2+" ID,"+organelle1+" Surface Area (nm^2),"+organelle2+" Surface Area (nm^2),,Total Objects\n");
		}
		boolean firstLine = true;
		for(Entry<Long,long[]> objectIDandInformation: collectedObjectInformation.entrySet()) {
			String outputString = Long.toString(objectIDandInformation.getKey());
			long [] objectInformation = objectIDandInformation.getValue();
			outputString+=","+Double.toString(objectInformation[0]*Math.pow(pixelDimension,3)); // volume
			outputString+=","+Double.toString(objectInformation[1]*Math.pow(pixelDimension,2)); //surface area
			outputString+=","+Double.toString(pixelDimension*objectInformation[2]/objectInformation[0]); //com x
			outputString+=","+Double.toString(pixelDimension*objectInformation[3]/objectInformation[0]); //com y
			outputString+=","+Double.toString(pixelDimension*objectInformation[4]/objectInformation[0]); //com z
			for(int i=5;i<11;i++) {
				outputString+=","+Double.toString(objectInformation[i]*pixelDimension);// min and max xyz
			}
			if(datasetNames.length>1) {
				outputString+=","+Long.toString(objectInformation[11])+","+Long.toString(objectInformation[12]);//organelle ids
				outputString+=","+Long.toString((long) (objectInformation[13]*Math.pow(pixelDimension,2)))+","+Long.toString((long)(objectInformation[14]*Math.pow(pixelDimension,2)));//organelle surface areas
			}
			if(firstLine) {
				outputString+=",,"+collectedObjectInformation.size()+"\n";
				firstLine = false;
			}
			else {
				outputString+=",\n";
			}
			csvWriter.append(outputString);
		}
		csvWriter.flush();
		csvWriter.close();

		boolean firstLineInAllCountsFile = false;
		if (! new File(outputDirectory+"/allCounts.csv").exists()) {
			firstLineInAllCountsFile = true;
		}
		csvWriter = new FileWriter(outputDirectory+"/allCounts.csv", true);
		if(firstLineInAllCountsFile) csvWriter.append("Object,Count\n");

		csvWriter.append(outputFile+","+collectedObjectInformation.size()+"\n");
		csvWriter.flush();
		csvWriter.close();
	}

	public static final String PAIRS_SUFFIX = "_pairs";
	public static final String TO_SEPARATOR = "_to_";
	public static final String CC_SUFFIX = "_cc";

	public static void setupSparkAndRunGeneralCosemObjectInformation(String inputN5DatasetName, String inputN5Path, String inputPairsString, String outputDirectory, boolean skipContactSites, boolean skipSelfContacts) throws IOException {
		// Get all organelles
		final SparkConf conf = new SparkConf().setAppName("SparkGeneralCosemInformation");

		String[] organelles = null;

		List<String[]> customOrganellePairs = parseInputPairs(inputPairsString);

		if (inputN5DatasetName != null) {
			organelles = inputN5DatasetName.split(",");
		}

		new File(outputDirectory+"/allCounts.csv").delete();

		if(organelles != null) {
			processOrganelles(organelles, inputN5Path, outputDirectory, conf);
		}

		//contact sites
		processContactSites(customOrganellePairs, organelles, inputN5Path, outputDirectory, skipContactSites, skipSelfContacts, conf);
	}

	private static List<String[]> parseInputPairs(String inputPairsString) {
		List<String[]> customOrganellePairs = new ArrayList<String[]>();
		if(inputPairsString !=null) {
			String[] inputPairs = inputPairsString.split(",");
			HashSet<String> organelleSet = new HashSet<String>();
			for(String pair : inputPairs) {
				String organelle1 = pair.split(TO_SEPARATOR)[0];
				String organelle2 = pair.split(TO_SEPARATOR)[1];
				organelleSet.add(organelle1);
				organelleSet.add(organelle2);
				customOrganellePairs.add(new String[] {organelle1, organelle2});
			}
		}
		return customOrganellePairs;
	}

	private static void processOrganelles(String[] organelles, String inputN5Path, String outputDirectory, SparkConf conf) throws IOException {
		System.out.println(Arrays.toString(organelles));
		for (String currentOrganelle : organelles) {
			System.out.println(currentOrganelle);
			String [] datasetNames = {currentOrganelle};
			calculateVolumeAreaCountForDataset(inputN5Path, datasetNames, outputDirectory, conf);
		}
	}

	private static void calculateVolumeAreaCountForDataset(String inputN5Path, String[] datasetNames, String outputDirectory, SparkConf conf) throws IOException {
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path, datasetNames[0]);
		calculateVolumeAreaCount(sc, inputN5Path, datasetNames, outputDirectory, blockInformationList);
		sc.close();
	}

	private static void processContactSites(List<String[]> customOrganellePairs, String[] organelles, String inputN5Path, String outputDirectory, boolean skipContactSites, boolean skipSelfContacts, SparkConf conf) throws IOException {
		if(customOrganellePairs.size()>0) { //custom contact sites
			for(String[] organellePair : customOrganellePairs) {
				String [] datasetNames = {organellePair[0],organellePair[0].equals(organellePair[1]) ? organellePair[1]+PAIRS_SUFFIX : organellePair[1] ,organellePair[0]+TO_SEPARATOR+organellePair[1]+CC_SUFFIX};
				System.out.println(Arrays.toString(datasetNames));
				calculateVolumeAreaCountForDataset(inputN5Path, datasetNames, outputDirectory, conf);
			}
		}
		else {
			if (!skipContactSites) {
				for (int i=0; i<organelles.length; i++) {
					for(int j= skipSelfContacts ? i+1 : i; j<organelles.length;j++) {
						String [] datasetNames = {organelles[i],i==j ? organelles[j]+"_pairs" : organelles[j] ,organelles[i]+"_to_"+organelles[j]+"_cc"};
						System.out.println(Arrays.toString(datasetNames));

						JavaSparkContext sc = new JavaSparkContext(conf);
						List<BlockInformation> blockInformationList = BlockInformation.buildBlockInformationList(inputN5Path, datasetNames[2]);
						calculateVolumeAreaCount(sc, inputN5Path, datasetNames, outputDirectory, blockInformationList);
						sc.close();
					}
				}
			}
		}
	}

	public static final void main(final String... args) throws IOException, InterruptedException, ExecutionException {

		final Options options = new Options(args);

		if (!options.parsedSuccessfully)
			return;

		String inputN5DatasetName = options.getInputN5DatasetName();
		String inputN5Path = options.getInputN5Path();
		String inputPairsString = options.getInputPairs();
		String outputDirectory = options.getOutputDirectory();
		boolean skipContactSites = options.getSkipContactSites();
		boolean skipSelfContacts = options.getSkipSelfContacts();

		setupSparkAndRunGeneralCosemObjectInformation(inputN5DatasetName, inputN5Path, inputPairsString, outputDirectory, skipContactSites, skipSelfContacts);
	}
}