import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileQueue {

	private final String baseDir;
	private final int segmentSize;
	private final long retentionPeriod; // milliseconds
	private String currentFile;
	private long currentFilePosition;
	private String checkpointFile;
	private Long checkpointPos;

	private void log(String s){
		System.out.println(s);
	}

	//"Record" subclass to hold offset, size and data
	public class ConsumerRecord {
		String checkpoint;
		int size;
		byte[] data;

		public ConsumerRecord(String checkpoint, int size, byte[] data) {
			this.checkpoint = checkpoint;
			this.size = size;
			this.data = data;
		}

		//getters
		public String getCheckpoint() {
			return checkpoint;
		}

		public int getSize() {
			return size;
		}

		public byte[] getData() {
			return data;
		}

	}


	public FileQueue(String baseDir, int segmentSize, long retentionPeriod) throws IOException {
		this.baseDir = baseDir;
		this.segmentSize = segmentSize;
		this.retentionPeriod = retentionPeriod;
		refreshPositions(); 		
		new Thread(() -> {
			while (true) { try {
				Thread.sleep(600000);
				System.out.println("Cleaning old files");
				cleanOldFiles();
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
			}
		}).start();

	}

	private String getEarliestSegment(){

		String earliestSegment = null;
		// sort the files in the directory and get the earliest file
		File dir = new File(baseDir);
		File[] files = dir.listFiles();
		long earliest = Long.MAX_VALUE;
		for (File file : files) {
			if (file.lastModified() < earliest && !file.getName().equals("checkpoint")) {
				earliest = file.lastModified();
				earliestSegment = file.getName();
			}
		}
		return appendBaseDirIfNotNull(earliestSegment);

	}

	private String getLatestSegment(){

		String latestSegment = null;
		// sort the files in the directory and get the latest file
		File dir = new File(baseDir);
		File[] files = dir.listFiles();
		long latest = Long.MIN_VALUE;
		for (File file : files) {
			if (file.lastModified() > latest && !file.getName().equals("checkpoint")) {
				latest = file.lastModified();
				latestSegment = file.getName();
			}
		}
		return appendBaseDirIfNotNull(latestSegment);

	}

	private String appendBaseDirIfNotNull(String fileName){
		return fileName == null ? null : baseDir + File.separator + fileName;
	}

	private void refreshPositions() throws IOException {

		//update checkpoint from file or create one with earliest segment
		if (Files.exists(Paths.get(baseDir + File.separator + "checkpoint"))) {
			String[] checkpoint = new String(Files.readAllBytes(Paths.get(baseDir + File.separator +"checkpoint"))).split(" ");
			this.checkpointFile = checkpoint[0];
			this.checkpointPos = Long.parseLong(checkpoint[1]);
		} else {
			String earliestSegment = getEarliestSegment();
			if(earliestSegment == null) {
				this.checkpointFile = createNewSegment();
				this.checkpointPos = 0L;
			}	
			this.checkpointPos = 0L;
			Files.write(Paths.get(baseDir + File.separator +"checkpoint"), (checkpointFile + " " + checkpointPos).getBytes(), StandardOpenOption.CREATE);
		}

		String latestSegment = getLatestSegment();
		if(latestSegment == null){
			latestSegment = createNewSegment();
		}
		this.currentFile = latestSegment;
		log("Latest segment : " + latestSegment);
		this.currentFilePosition = Files.size(Paths.get(currentFile));
	}


	private void maySelectNewSegment() throws Exception{
		if(currentFile == null || !Files.exists(Paths.get(currentFile))) {
			currentFile = chooseNextSegment(currentFile);
			//update current file position to start of the next segment
			currentFilePosition = 0;
			if(currentFile == null) {
				currentFile = getNextNewFileName();
				// if there are no next segments, then create the new segment
				if (!Files.exists(Paths.get(currentFile))) {
					Files.createFile(Paths.get(currentFile));
				}
			}
		}
		log("Selected new segment : " + currentFile);
	}



	public synchronized void push(byte[] data) throws Exception {
		if (data.length > segmentSize) {
			throw new IllegalArgumentException("Data exceeds segment size");
		}
		Path currentPath = Paths.get(currentFile);

		long fileSize = 0;
		if(Files.exists(currentPath)){
			fileSize = Files.size(currentPath);
		} else {
			this.currentFile = createNewSegment();
			this.currentFilePosition = 0;
		}

		// if data to add is greater than remaining segment size, then create new segment
		if (fileSize + data.length + 4 > segmentSize) {
			this.currentFile = createNewSegment();
			this.currentFilePosition = 0;
		}

		//append size of the data to the data file in first 4 bytes and then append the data
		byte[] size = new byte[4];
		size[3] = (byte) (data.length >> 24);
		size[2] = (byte) (data.length >> 16);
		size[1] = (byte) (data.length >> 8);
		size[0] = (byte) data.length;

		Files.write(Paths.get(currentFile), size, StandardOpenOption.APPEND);
		log("Wrote file size : " + size);
		Files.write(Paths.get(currentFile), data, StandardOpenOption.APPEND);
		log("Wrote data : " + data);
	}

	public ConsumerRecord poll() throws Exception {
		byte[] data = dequeue();
		if (data != null) {
			return new ConsumerRecord(checkpointFile + " " + checkpointPos , data.length, data);
		}
		return null;
	}


	private synchronized byte[] dequeue() throws Exception {

		String fileToPoll = checkpointFile;
		Long pos = checkpointPos;
		log("File to poll : " + fileToPoll);
		if(!Files.exists(Paths.get(checkpointFile)) || checkpointPos == Files.size(Paths.get(checkpointFile))) {
			log("No more data in current segment");
			fileToPoll = chooseNextSegment(checkpointFile);
			pos = 0L;
			log("Next file to poll : " + fileToPoll);
			if(fileToPoll == null){
				log("Queue is empty");
				return null;
			}
		}

		Path currentPath = Paths.get(fileToPoll);
		try (RandomAccessFile raf = new RandomAccessFile(currentPath.toFile(), "r")) {
			long fileSize = raf.length();
			if (fileSize <= 4 || pos + 4 >= fileSize) {
				log("_Queue is empty");
				return null;
			}

			raf.seek(pos);

			byte[] size = new byte[4];
			raf.readFully(size);

			//decode int from the 4 bytes
			int messageSize = size[0] & 0xFF | (size[1] & 0xFF) << 8 | (size[2] & 0xFF) << 16 | (size[3] & 0xFF) << 24;
			//normalise message size in case of negative value
			if (messageSize < 0) {
				messageSize = messageSize & 0x7FFFFFFF;
			}
			byte[] data = new byte[messageSize];
			raf.seek(pos + 4);
			raf.readFully(data);
			return data;
		}
	}

	private int move(int messageCount) throws Exception{
		int noOfBytesToMove = 0;
		try (RandomAccessFile raf = new RandomAccessFile(checkpointFile, "r")) {
			for (int i = 0; i < messageCount; i++) {
				raf.seek(checkpointPos + noOfBytesToMove);
				byte[] size = new byte[4];
				raf.readFully(size);
				int messageSize = size[0] & 0xFF | (size[1] & 0xFF) << 8 | (size[2] & 0xFF) << 16 | (size[3] & 0xFF) << 24;
				if (messageSize < 0) {
					messageSize = messageSize & 0x7FFFFFFF;
				}
				log("Message Size : " + messageSize);
				noOfBytesToMove += 4 + messageSize;
				if (raf.length() < checkpointPos + noOfBytesToMove) {
					throw new Exception("No more messages to commit " + raf.length() + " " + checkpointFile +" " + noOfBytesToMove);
				}
			}
			this.checkpointPos +=  noOfBytesToMove;
			Files.write(Paths.get(baseDir+File.separator+"checkpoint"), (checkpointFile + " " + checkpointPos).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);	
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return noOfBytesToMove;
	}


	public void commit(String checkpoint) throws Exception {
		if (!checkpoint.equals(checkpointFile + " " + checkpointPos)){
			throw new Exception("Invalid checkpoint");
		}
		log("Checkpoint file :" + this.checkpointFile);
		if(!Files.exists(Paths.get(this.checkpointFile)) || Files.size(Paths.get(this.checkpointFile)) == checkpointPos) {
			// find next segment 
			String nextCheckPointFile = chooseNextSegment(checkpointFile);
			if(nextCheckPointFile == null){
				nextCheckPointFile = createNewSegment();
			}
			this.checkpointFile = nextCheckPointFile;
			this.checkpointPos = 0L;
			Files.write(Paths.get(baseDir+File.separator+"checkpoint"), (this.checkpointFile + " " + this.checkpointPos).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);	
			log("Overwritten checkpoint file with " + checkpointFile + " " + checkpointPos);
		}

		log("Moving " + this.checkpointFile + " : " + checkpointPos + " : " + Files.size(Paths.get(this.checkpointFile)));
		move(1);

	}

	private String createNewSegment() throws IOException { 
		String newSegment = getNextNewFileName();
		Files.createFile(Paths.get(newSegment));
		log("new segment : " + newSegment);
		return newSegment;
	}

	private String getNextNewFileName() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		return baseDir + File.separator + sdf.format(new Date()) + ".log";
	}

	private void cleanOldFiles() throws IOException {
		long threshold = System.currentTimeMillis() - retentionPeriod;
		File dir = new File(baseDir);
		if (!dir.exists()) {
			return;
		}

		for (File file : dir.listFiles()) {
			if (file.isFile() && file.lastModified() < threshold && !file.getName().equals("checkpoint")) {
				log("Deleting " + file.getName());
				Files.delete(file.toPath());
			}
		}
	}

	private String chooseNextSegment(String currentFile) {
		log("Choosing next segment");
		File dir = new File(baseDir);
		File[] files = dir.listFiles();

		Stream<File> fileList = Arrays.stream(files).filter(file -> !(file.getName()).equals("checkpoint"));
		if(currentFile != null) {
			fileList = fileList.filter(file ->(0 < appendBaseDirIfNotNull(file.getName()).compareTo(currentFile)));
		}
		List<File> sortedList =	fileList.sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
		String nextSegment = sortedList.size() > 0 ? sortedList.get(0).getName() : null;
		log("Chosen next segment : " + nextSegment + " : " + sortedList.size());
		return appendBaseDirIfNotNull(nextSegment);

	}



	public static void main(String[] args) throws Exception {

		FileQueue queue = new FileQueue("logs",10 , 60000); // 1 min

		// wait for user input for enque and deque
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			System.out.println("Enter 1 to push, 2 to poll and 3 to commit");
			String input = reader.readLine();
			String message;
			if (input.equals("1")) {
				System.out.println("Enter message to enqueue");
				message = reader.readLine();
				queue.push(message.getBytes());
			} else if (input.equals("2")) {
				ConsumerRecord consumerRecord = queue.poll();
				if (consumerRecord != null) {
					System.out.println("Message: " + new String(consumerRecord.getData()));
					System.out.println("Checkpoint: " + consumerRecord.getCheckpoint());
					System.out.println("Size: " + consumerRecord.getSize());
				} else {
					System.out.println("Queue is empty");
				}
			} else if (input.equals("3")) {
				System.out.println("Enter number of messages to commit");
				String checkpoint = reader.readLine();
				try{
					queue.commit(checkpoint);
				}catch (Exception e) {
					System.out.println(e.getMessage());
				}
			}
		}
	}
}
