
bool is_GPU();

void init_blocks();

int new_block(size_t t, size_t len);

void* get_block(int loc);

void delete_block(int loc);
