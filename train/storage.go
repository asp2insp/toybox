package track

// The track package is responsible for recording messages to a set of files.
// Each file holds CHUNK_SIZE messages, except for the active file which begins empty and grows to hold
// up to CHUNK_SIZE messages. Messages are stored in their entirety, with their wrapping.
