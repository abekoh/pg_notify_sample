import ws from 'k6/ws';
import {check} from 'k6';
import {uuidv4,sleep} from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const roomIds = [uuidv4(), uuidv4(), uuidv4()];

export const options = {
    vus: 20,
    duration: '10s',
};

export default function () {
    const roomId = roomIds[Math.floor(Math.random() * roomIds.length)];
    const url = `ws://localhost:8080/ws?room_id=${roomId}`;
    const params = {};

    let receiveCount = 0;
    const res = ws.connect(url, params, (socket) => {
        socket.on('open', () => {
            for (let i = 0; i < 1000; i++) {
                socket.send(JSON.stringify(generateRandomJSON()));
            }
            socket.setTimeout(() => {
                socket.close()
            }, 1000);
        });
    });
    check(res, {'status is 101': (r) => r && r.status === 101});
    console.log(`Received ${receiveCount} messages`);
}

function generateRandomString(maxLength) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const length = Math.floor(Math.random() * maxLength) + 1;
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
}

function generateRandomJSON(depth = 2, maxKeys = 3, maxValueLength = 50) {
    if (depth === 0) {
        return generateRandomString(maxValueLength);
    } else {
        const keysCount = Math.floor(Math.random() * maxKeys) + 1;
        let obj = {};
        for (let i = 0; i < keysCount; i++) {
            obj[generateRandomString(maxValueLength)] = generateRandomJSON(depth - 1, maxKeys, maxValueLength);
        }
        return obj;
    }
}