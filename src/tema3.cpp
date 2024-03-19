#include <mpi.h>
#include <pthread.h>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>

/* Defined limits */
#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

/* Tag codes */
#define INIT_TAG 0
#define START_TAG 1
#define REQUEST_TO_TRACKER 2
#define REQUEST_TO_PEER 3
#define ACK_TAG 4
#define UPDATE_SWARM_TAG 5

/* Request types */
#define FILE_REQUEST 0
#define FINISH_ALL_DOWNLOADS_REQUEST 1
#define FINISH_UPLOAD_REQUEST 2
#define HASH_REQUEST 3
#define UPDATE_COLLECTION_REQUEST 4

using namespace std;

typedef pair< string, vector<string> > File;                        /* File name and corresponding hashes */
typedef map< string, pair< vector<string>, bool > > FileContainer;  /* For each file name, store its hashes and if the file is complete */
typedef map< int, FileContainer > SeederFileList;                   /* Store files for each client */
typedef map< string, pair< vector<int>, int > > Swarm;              /* Store seeders for every hash and its position in the file */

/* Given as argument to peer threads */
typedef struct {
    int rank;
    FileContainer *ownFiles;
    vector<string> *wantedFiles;
} Seeder;

/* Read owned files from input file */
FileContainer readOwnFiles(ifstream& in) {
    FileContainer files;

    int ownFileNo = 0;
    in >> ownFileNo;
    for(int i = 0; i < ownFileNo; ++i) {
        string filename;
        vector<string> hashes;

        int hashNum;
        in >> filename >> hashNum;
        for(int j = 0; j < hashNum; ++j) {
            string hash;
            in >> hash;
            hashes.push_back(hash);
        }

        // Mark file as complete
        files[filename] = make_pair(hashes, true);
    }
    
    return files;
}

/* Read wanted files from input file */
vector<string> readWantedFiles(ifstream& in) {
    vector<string> wantedFiles;
    int wantedFilesNo;
    in >> wantedFilesNo;
    for(int i = 0; i < wantedFilesNo; ++i) {
        string fileName;
        in >> fileName;
        wantedFiles.push_back(fileName);
    }
    return wantedFiles;
}

/* Write downloaded file */
void writeFile(int client, string fileName, vector<string>& hashes) {
    ofstream out("client" + to_string(client) + "_" + fileName);

    for(auto i = hashes.begin(); i != hashes.end(); ++i)
        out << *i << "\n";
}

/* Send file to a client with a certain tag */
void sendFile(string fileName, vector<string>& hashes, int receiver, int tag) {
    int fileNameSize = fileName.size() + 1;
    MPI_Send(&fileNameSize, 1, MPI_INT, receiver, tag, MPI_COMM_WORLD);
    MPI_Send(fileName.c_str(), fileNameSize, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
    int hashNo = hashes.size();
    MPI_Send(&hashNo, 1, MPI_INT, receiver, tag, MPI_COMM_WORLD);
    for(auto j = hashes.begin(); j != hashes.end(); ++j) {
        string aux = *j;
        MPI_Send(aux.c_str(), HASH_SIZE + 1, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
    }
    bool all;
    MPI_Send(&all, 1, MPI_CXX_BOOL, receiver, tag, MPI_COMM_WORLD);
}

/* Send a all files owned to a client with a certain tag */
void sendFileContainer(FileContainer& files, int receiver, int tag) {
    int ownFileNo = files.size();

    MPI_Send(&ownFileNo, 1, MPI_INT, receiver, tag, MPI_COMM_WORLD);
    for(auto i = files.begin(); i != files.end(); ++i) {
        sendFile(i->first, i->second.first, receiver, tag);
        bool all = i->second.second;
        MPI_Send(&all, 1, MPI_CXX_BOOL, receiver, tag, MPI_COMM_WORLD);
    }
}

/* Receive file from a client with a certain tag */
File receiveFile(int sender, int tag) {
    int fileNameSize;
    MPI_Recv(&fileNameSize, 1, MPI_INT, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    char aux[fileNameSize];
    MPI_Recv(&aux, fileNameSize, MPI_CHAR, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    string fileName(aux);
    int hashNo;
    MPI_Recv(&hashNo, 1, MPI_INT, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    vector<string> hashes;
    for(int k = 0; k < hashNo; ++k) {
        char aux[HASH_SIZE + 1];
        MPI_Recv(&aux, HASH_SIZE + 1, MPI_CHAR, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        string hash(aux);
        hashes.push_back(hash);
    }
    bool all;
    MPI_Recv(&all, 1, MPI_CXX_BOOL, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    return make_pair(fileName, hashes);
}

/* Receive all files owned from a client with a certain tag */
FileContainer receiveFileContainer(int sender, int tag) {
    FileContainer files;
    int fileNo;
    MPI_Recv(&fileNo, 1, MPI_INT, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for(int j = 0; j < fileNo; ++j) {
        File file = receiveFile(sender, tag);
        bool all;
        MPI_Recv(&all, 1, MPI_CXX_BOOL, sender, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        files[file.first] = make_pair(file.second, all);
    }
    return files;
}

/* Receive a list of hashes for a file and who has each of them */
Swarm receiveHashHoldersForFile(string fileName) {
    int fileNameSize = fileName.size() + 1;
    MPI_Send(&fileNameSize, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
    MPI_Send(fileName.c_str(), fileNameSize, MPI_CHAR, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);    

    int hashNo = 0;
    MPI_Recv(&hashNo, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    Swarm hashHolders;

    for(int i = 0; i < hashNo; ++i) {
        char aux[HASH_SIZE + 1];
        MPI_Recv(&aux, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        string hash(aux);
        int holderNo;
        MPI_Recv(&holderNo, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        vector<int> holders;
        for(int j = 0; j < holderNo; ++j) {
            int holder;
            MPI_Recv(&holder, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            holders.push_back(holder);
        }
        int index;
        MPI_Recv(&index, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        hashHolders[hash] = make_pair(holders, index);
    }
    return hashHolders;
}

/* Send a list of hashes for a file and who has each of them */
void sendHashHoldersForFile(int receiver, SeederFileList seederFiles, map< string, Swarm >& swarms) {
    int fileNameSize;
    MPI_Recv(&fileNameSize, 1, MPI_INT, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    char aux[fileNameSize];
    MPI_Recv(&aux, fileNameSize, MPI_CHAR, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    string fileName(aux);

    Swarm hashHolders = swarms[fileName];

    int hashNo = hashHolders.size();
    MPI_Send(&hashNo, 1, MPI_INT, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD);


    for(auto i = hashHolders.begin(); i != hashHolders.end(); ++i) {
        string hash = i->first;
        vector<int> holders = i->second.first;
        int index = i->second.second;
        MPI_Send(hash.c_str(), HASH_SIZE + 1, MPI_CHAR, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
        int holdersNo = holders.size();
        MPI_Send(&holdersNo, 1, MPI_INT, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
        for(auto j = holders.begin(); j != holders.end(); ++j) {
            int holder = *j;
            MPI_Send(&holder, 1, MPI_INT, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
        }
        MPI_Send(&index, 1, MPI_INT, receiver, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
    }
}

/* Receive an update from a client and update the swarms for for each hash */
void updateSwarmOnReceive(int client, SeederFileList& seederFiles, map< string, Swarm >& swarms) {
    FileContainer files = receiveFileContainer(client, UPDATE_SWARM_TAG);
    seederFiles[client] = files;
    for(auto i = files.begin(); i != files.end(); ++i) {
        string fileName = i->first;
        Swarm swarm = swarms[fileName];
        vector<string> hashes = i->second.first;
        for(auto j = hashes.begin(); j != hashes.end(); ++j) {
            string hash = *j;
            if(find(swarm[hash].first.begin(), swarm[hash].first.end(), client) != swarm[hash].first.end())
                swarm[hash].first.push_back(client);
        }
    }
}

/* Create the swarm for each file */
map< string, Swarm > initSwarm(SeederFileList& seederFiles) {
    map< string, Swarm > swarms;
    for(auto o = seederFiles.begin(); o != seederFiles.end(); ++o) {
        int client = o->first;
        FileContainer& files = o->second;
        for(auto j = files.begin(); j != files.end(); ++j) {
            string fileName = j->first;
            Swarm hashHolders;
            if(swarms.count(fileName))
                hashHolders = swarms[fileName];
            vector<string> hashes = j->second.first;
            int idx = 0;
            for(auto k = hashes.begin(); k != hashes.end(); ++k) {
                if(hashHolders.count(*k)) {
                    hashHolders[*k].first.push_back(client);
                }
                else {
                    hashHolders[*k].first = {client};
                    hashHolders[*k].second = idx;
                }
                idx++;
            }
            if(!swarms.count(fileName))
                swarms[fileName] = hashHolders;
        }
    }
    return swarms;
}

void *download_thread_func(void *arg)
{
    Seeder seeder = *(Seeder*) arg;
    FileContainer& files = *seeder.ownFiles;
    int downloaded = 0, req;

    // Receive start signal
    int start;
    MPI_Recv(&start, 1, MPI_INT, TRACKER_RANK, START_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for(auto i = seeder.wantedFiles->begin(); i != seeder.wantedFiles->end(); ++i) {
        string wantedFile = *i;

        // Send a request for hashes to tracker
        req = FILE_REQUEST;
        MPI_Send(&req, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);

        Swarm hashHolders = receiveHashHoldersForFile(wantedFile);
        vector<string> hashes(hashHolders.size());

        files[wantedFile] = make_pair(hashes, false);
        for(auto i = hashHolders.begin(); i != hashHolders.end(); ++i) {
            string hash = i->first;
            vector<int>& holders = i->second.first;
            
            // Select peer to download the hash from
            int chosen = holders.at(rand() % holders.size());

            // Send a request for a hash to the peer
            req = HASH_REQUEST;
            MPI_Send(&req, 1, MPI_INT, chosen, REQUEST_TO_PEER, MPI_COMM_WORLD);
            MPI_Send(hash.c_str(), HASH_SIZE + 1, MPI_CHAR, chosen, REQUEST_TO_PEER, MPI_COMM_WORLD);
            int ack;
            MPI_Recv(&ack, 1, MPI_INT, chosen, ACK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            files[wantedFile].first[i->second.second] = hash;
 
            // Send an update to the tracker after every 10 segments and receive update
            downloaded++;
            if(downloaded == 10) {
                downloaded = 0;
                
                req = UPDATE_COLLECTION_REQUEST;
                MPI_Send(&req, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
                
                sendFileContainer(files, TRACKER_RANK, UPDATE_SWARM_TAG);

                req = FILE_REQUEST;
                MPI_Send(&req, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);

                int idx = distance(hashHolders.begin(), i);
                hashHolders = receiveHashHoldersForFile(wantedFile);
                i = hashHolders.begin();
                advance(i, idx);
            }
        }
        
        // Mark file as complete and write it
        files[wantedFile] = make_pair(files[wantedFile].first, true);
        writeFile(seeder.rank, wantedFile, files[wantedFile].first);

        req = UPDATE_COLLECTION_REQUEST;
        MPI_Send(&req, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);
        sendFileContainer(files, TRACKER_RANK, UPDATE_SWARM_TAG);
    }

    // Inform the tracker that all files have been downloaded
    req = FINISH_ALL_DOWNLOADS_REQUEST;
    MPI_Send(&req, 1, MPI_INT, TRACKER_RANK, REQUEST_TO_TRACKER, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    Seeder seeder = *(Seeder*) arg;

    // Send files owned by client to tracker
    sendFileContainer(*seeder.ownFiles, TRACKER_RANK, INIT_TAG);

    // Wait for requests from other clients
    int req, finish = 0;
    MPI_Status status;
    while(!finish) {
        MPI_Recv(&req, 1, MPI_INT, MPI_ANY_SOURCE, REQUEST_TO_PEER, MPI_COMM_WORLD, &status);
        switch(req) {
            case HASH_REQUEST: {
                // Receive requested hash
                char hash[HASH_SIZE + 1];
                MPI_Recv(&hash, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, REQUEST_TO_PEER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Simulate sending the hash
                int ack = 1;
                MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, ACK_TAG, MPI_COMM_WORLD);
                break;
            }
            case FINISH_UPLOAD_REQUEST:
                finish = 1;
                break;
            default:
                break;
        }
    }

    return NULL;
}

void tracker(int numtasks, int rank) {
    map< string, Swarm > swarms;
    SeederFileList seederFiles;

    // Receive initial files from all clientss
    for(int i = 1; i < numtasks; ++i)
        seederFiles[i] = receiveFileContainer(i, INIT_TAG);

    swarms = initSwarm(seederFiles);

    // Send start signal to all clients to start downloading
    int start = 1;
    for(int i = 1; i < numtasks; ++i)
        MPI_Send(&start, 1, MPI_INT, i, START_TAG, MPI_COMM_WORLD);

    int downloadersLeft = numtasks - 1;

    // Wait for requests from clients
    int req;
    MPI_Status status;
    while (downloadersLeft > 0) {
        MPI_Recv(&req, 1, MPI_INT, MPI_ANY_SOURCE, REQUEST_TO_TRACKER, MPI_COMM_WORLD, &status);
        switch(req) {
            case FILE_REQUEST: {
                // Send the list of hashes and who has each of them
                sendHashHoldersForFile(status.MPI_SOURCE, seederFiles, swarms);
                break;
            }
            case UPDATE_COLLECTION_REQUEST: {
                // Receive an update from a client
                updateSwarmOnReceive(status.MPI_SOURCE, seederFiles, swarms);
                break;
            }
            case FINISH_ALL_DOWNLOADS_REQUEST: {
                // A client has finished all its downloads
                downloadersLeft--;
                break;
            }
            default:
                break;
        }
    }

    req = FINISH_UPLOAD_REQUEST;
    for(int i = 1; i < numtasks; ++i)
        MPI_Send(&req, 1, MPI_INT, i, REQUEST_TO_PEER, MPI_COMM_WORLD);
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    // Open input file and read files
    ifstream in("in" + to_string(rank) + ".txt");
    FileContainer ownFiles = readOwnFiles(in);
    vector<string> wantedFiles = readWantedFiles(in);

    Seeder seeder;
    seeder.rank = rank;
    seeder.wantedFiles = &wantedFiles;
    seeder.ownFiles = &ownFiles;

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &seeder);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &seeder);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
