#ifndef _CONTEXT_H_
#define _CONTEXT_H_

#include "types.h"

void getSCAddress(byte *address);
void getOwnerAddress(byte *address);
int getShardOfAddress(byte *address);
int isSmartContract(byte *address);

// EllipticCurve-Related functions
void addEC(int xResultHandle, int yResultHandle, int ecHandle, int fstPointXHandle, int fstPointYHandle, int sndPointXHandle, int sndPointYHandle);
void doubleEC(int xResultHandle, int yResultHandle, int ecHandle, int pointXHandle, int pointYHandle);
int isOnCurveEC(int ecHandle, int pointXHandle, int pointYHandle);
int scalarBaseMultEC(int xResultHandle, int yResultHandle, int ecHandle, byte *dataOffset, int length);
int scalarMultEC(int xResultHandle, int yResultHandle, int ecHandle, int pointXHandle, int pointYHandle, byte *dataOffset, int length);
int marshalEC(int xPairHandle, int yPairHandle, int ecHandle, byte *resultOffset);
int unmarshalEC(int xResultHandle, int yResultHandle, int ecHandle, byte *dataOffset, int length);
int marshalCompressedEC(int xPairHandle, int yPairHandle, int ecHandle, byte *resultOffset);
int unmarshalCompressedEC(int xResultHandle, int yResultHandle, int ecHandle, byte *dataOffset, int length);
int generateKeyEC(int xPubKeyHandle, int yPubKeyHandle, int ecHandle, byte *resultOffset);
int p224Ec();
int p256Ec();
int p384Ec();
int p521Ec();
int getCurveLengthEC(int ecHandle);
int getPrivKeyByteLengthEC(int ecHandle);
int ellipticCurveGetValues(int ecHandle, int fieldOrderHandle, int basePointOrderHandle, int eqConstantHandle, int xBasePointHandle, int yBasePointHandle);

// Call-related functions
void getCaller(byte *callerAddress);
int getFunction(byte *function);
int getCallValue(byte *result);
long long getGasLeft();
void finish(byte *data, int length);
void int64finish(long long value);
void writeLog(byte *pointer, int length, byte *topicPtr, int numTopics);
void asyncCall(byte *destination, byte *value, byte *data, int length);
void signalError(byte *message, int length);

int executeOnSameContext(long long gas, byte *address, byte *value, byte *function, int functionLength, int numArguments, byte *argumentsLengths, byte *arguments);
int executeOnDestContext(long long gas, byte *address, byte *value, byte *function, int functionLength, int numArguments, byte *argumentsLengths, byte *arguments);
int createContract(long long gas, byte *value, byte *code, byte *codeMetadata, int codeSize, byte *newAddress, int numInitArgs, byte *initArgLengths, byte *initArgs);

// Blockchain-related functions
long long getBlockTimestamp();
int getBlockHash(long long nonce, byte *hash);

// Argument-related functions
int getNumArguments();
int getArgument(int argumentIndex, byte *argument);
long long int64getArgument(int argumentIndex);
int getArgumentLength(int argumentIndex);

// Account-related functions
void getExternalBalance(byte *address, byte *balance);
int transferValue(byte *destination, byte *value, byte *data, int length);

// Storage-related functions
int storageLoadLength(byte *key, int keyLength);
int storageStore(byte *key, int keyLength, byte *data, int dataLength);
int storageLoad(byte *key, int keyLength, byte *data);
int int64storageStore(byte *key, int keyLength, long long value);
long long int64storageLoad(byte *key, int keyLength);

// Timelocks related functions
int setStorageLock(byte *key, int keyLen, long long timeLock);
long long getStorageLock(byte *key, int keyLen);
int isStorageLocked(byte *key, int keyLen);
int clearStorageLock(byte *key, int keyLen);

int sha256(byte * dataOffset, int length, byte * resultOffset);
int keccak256(byte * dataOffset, int length, byte * resultOffset);
int ripemd160(byte * dataOffset, int length, byte * resultOffset);
int verifyBLS(byte * keyOffset, byte * messageOffset, int messageLength, byte * sigOffset);
int verifyEd25519(byte * keyOffset, byte * messageOffset, int messageLength, byte * sigOffset);
int verifySecp256k1(byte * keyOffset, int keyLength, byte * messageOffset, int messageLength, byte * sigOffset);

#endif
