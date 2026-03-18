#include <iostream>
#include <list>
#include <queue>
#include <cstdint>

#define MAX_QUEUES 64
#define MEMORY_SIZE 2048
#define CHUNK_SIZE 64


struct Q {
	std::uint16_t head;
	std::uint16_t tail;
	std::uint16_t index_position;
	std::uint16_t read_offset;
	bool destroyed = false;
};

const uint16_t DATA_SIZE = CHUNK_SIZE - sizeof(std::uint16_t) - sizeof(std::uint16_t);

struct DataChunk {
	unsigned char data[DATA_SIZE];
	std::uint16_t next;
	std::uint16_t size;  // Number of bytes enqueued to this chunk
};

static std::uint16_t free_list_head = 0;

const uint16_t METADATA_POOL_SIZE = MAX_QUEUES * (sizeof(Q) / sizeof(unsigned char));
const uint16_t DATA_POOL_START = METADATA_POOL_SIZE;
const uint16_t TOTAL_CHUNKS = (MEMORY_SIZE - DATA_POOL_START) / CHUNK_SIZE;

unsigned char data[MEMORY_SIZE];

static std::uint16_t queue_counter = 0;

void init_chunks() {

	free_list_head = DATA_POOL_START;

	for (uint16_t i = 0; i < TOTAL_CHUNKS; ++i)
	{
		std::uint16_t current_chunk_index = DATA_POOL_START + (i * CHUNK_SIZE);

		DataChunk* current_chunk = reinterpret_cast<DataChunk*>(&data[current_chunk_index]);
		
		std::uint16_t next_chunk_index = DATA_POOL_START + ((i+1) * CHUNK_SIZE);

		if (i < TOTAL_CHUNKS - 1)
		{
			current_chunk->next = next_chunk_index;
		}
		else
		{
			current_chunk->next = 0;
		}
		// std::cout << "Chunk: " << i << " created. It points to: " << current_chunk->next << std::endl;
	}

	// std::cout << "Memory allocated. " << "Free list head: " << free_list_head << std::endl;
}

// Handle out of memory exceptions
void on_out_of_memory() {
	std::cout << "Out of memory\n";
	std::exit(EXIT_FAILURE);
}
// Handle illegal requests
void on_illegal_operation() {
	std::cout << "Illegal operation\n";
	std::exit(EXIT_FAILURE);
}

// Creates a FIFO byte queue, returning a handle to it.
Q* create_queue() {

	if (queue_counter >= MAX_QUEUES) {
		on_out_of_memory();
	}

	std::uint16_t metadata_id = queue_counter;

	uint16_t q_byte_index = metadata_id * (sizeof(Q) / sizeof(unsigned char));
	Q* q_ptr = reinterpret_cast<Q*>(&data[q_byte_index]);

	if (free_list_head == 0) {
		on_out_of_memory();
	}

	uint16_t chunk_index = free_list_head;

	DataChunk* current_chunk = reinterpret_cast<DataChunk*>(&data[chunk_index]);

	free_list_head = current_chunk->next;

	current_chunk->next = 0;
	current_chunk->size = 0;

	q_ptr->head = chunk_index;
	q_ptr->tail = chunk_index;
	q_ptr->index_position = 0;
	q_ptr->read_offset = 0;

	// std::cout << "Queue created at: " << q_ptr << ". Head at: " << q_ptr->head << ". Tail at: " << q_ptr->tail << ". Size: " << q_ptr->size << std::endl;

	queue_counter++;
	return q_ptr;
}
// Destroy an earlier created byte queue.
void destroy_queue(Q* q) {

	if (q->destroyed) {
		on_illegal_operation();
	}

	std::uint16_t current_index = q->head;

	// Iteruj přes všechny chunky a vrať je do free list
	if (current_index != 0) {
		while (current_index != 0) {
			DataChunk* current_chunk = reinterpret_cast<DataChunk*>(&data[current_index]);
			std::uint16_t next_index = current_chunk->next;
			current_chunk->next = free_list_head;
			free_list_head = current_index;
			current_index = next_index;
		}
	}

	q->head = 0;
	q->tail = 0;
	q->index_position = 0;
	q->read_offset = 0;
	q->destroyed = true;

	// std::cout << "Queue " << q << " successfully destroyed" << std::endl;
}
// Adds a new byte to a queue.
void enqueue_byte(Q* q, unsigned char b) {

	if (q->destroyed)
	{
		on_illegal_operation();
	}

	DataChunk* current_chunk = reinterpret_cast<DataChunk*>(&data[q->tail]);
	std::int32_t true_size_signed = (std::int32_t)q->index_position - (std::int32_t)q->read_offset;
	std::uint16_t true_size = (true_size_signed >= 0) ? (std::uint16_t)true_size_signed : 0;
	std::uint16_t byte_offset = true_size % DATA_SIZE;

	if ((byte_offset == 0 && true_size > 0) || (true_size_signed < 0)) {
		std::uint16_t new_chunk_index = free_list_head;

		if (new_chunk_index == 0) {
			on_out_of_memory();
		}

		DataChunk* new_chunk = reinterpret_cast<DataChunk*>(&data[new_chunk_index]);

		free_list_head = new_chunk->next;
		new_chunk->next = 0;
		new_chunk->size = 0;

		current_chunk->next = new_chunk_index;
		q->tail = new_chunk_index;

		current_chunk = new_chunk;
		byte_offset = 0;
	}

	current_chunk->data[byte_offset] = b;
	current_chunk->size = byte_offset + 1;
	q->index_position++;

	// std::cout << "Enqueued '" << (int)b << "'. Tail at " << q_ptr->tail << ". Local offset " << byte_offset << ". New size: " << q_ptr->size << std::endl;
}
// Pops the next byte off the FIFO queue.
unsigned char dequeue_byte(Q* q) {

	if (q->destroyed || q->index_position == 0) {
		on_illegal_operation();
	}

	DataChunk* head_chunk = reinterpret_cast<DataChunk*>(&data[q->head]);
	unsigned char dequeued_byte = head_chunk->data[q->read_offset];

	q->read_offset++;
	q->index_position--;

	// Check if we've read all bytes from this chunk
	if (q->read_offset >= head_chunk->size) {
		if (q->index_position == 0) {
			q->head = 0;
			q->tail = 0;
			q->read_offset = 0;
		} else {
			std::uint16_t old_chunk_index = q->head;

			q->head = head_chunk->next;

			head_chunk->next = free_list_head;
			free_list_head = old_chunk_index;

			q->read_offset = 0;
		}
	}
	return dequeued_byte;
}

int main()
{	
	init_chunks();

	Q* q0 = create_queue();
	enqueue_byte(q0, 0);
	enqueue_byte(q0, 1);
	Q* q1 = create_queue();
	enqueue_byte(q1, 3);
	enqueue_byte(q0, 2);
	enqueue_byte(q1, 4);
	printf("%d", dequeue_byte(q0));
	printf("%d\n", dequeue_byte(q0));
	enqueue_byte(q0, 5);
	enqueue_byte(q1, 6);
	printf("%d", dequeue_byte(q0));
	printf("%d\n", dequeue_byte(q0));
	destroy_queue(q0);
	printf("%d", dequeue_byte(q1));
	printf("%d", dequeue_byte(q1));
	printf("%d\n", dequeue_byte(q1));
	destroy_queue(q1);

	std::uint16_t DATA_SEGMENT = TOTAL_CHUNKS * DATA_SIZE;
	std::uint16_t POINTER_SEGMENT = TOTAL_CHUNKS * sizeof(std::uint16_t);
	std::cout << "\nMemory Usage:\n";
	std::cout << "[Metadata: " << METADATA_POOL_SIZE << " bytes]\n";
	std::cout << "[Data capacity: " << DATA_SEGMENT << " bytes]\n";
	std::cout << "[Pointer overhead: " << POINTER_SEGMENT << " bytes]\n";
	std::cout << "[Total allocated: " << MEMORY_SIZE << " bytes]\n";
}
