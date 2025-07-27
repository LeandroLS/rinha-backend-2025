import Fastify from 'fastify';
const fastify = Fastify({
    logger: true
});
const schema = {
    body: {
        type: 'object',
        required: ['correlationId', 'amount'],
        properties: {
            amount: { type: 'number', multipleOf: 0.01, minimum: 0.01 },
            correlationId: { type: 'string', format: 'uuid' },
        }
    },
    response: {
        200: {
            type: 'null'
        }
    }
};
fastify.post('/payments', { schema }, async function handler(request, reply) {
    console.log("== passei aqui ==");
    reply.code(200).send();
});
try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
}
catch (err) {
    fastify.log.error(err);
    process.exit(1);
}
