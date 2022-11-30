FROM node:19 AS frontend

COPY ./frontend/ ./frontend/

RUN cd frontend && npm install && npm run build

FROM node:19 AS SERVER

COPY ./backend/ ./backend/
COPY --from=frontend ./frontend/dist/ ./backend/dist/
RUN cd backend && npm install

EXPOSE 3000

CMD ["node", "./backend/backend.js"]