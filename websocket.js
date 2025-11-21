require('dotenv').config();
const WebSocket = require('ws');
const mysql = require('mysql2/promise');

const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  timezone: '+00:00'
};

function arredondarCustom(valor) {
    if (isNaN(valor)) valor = 0;
    const sinal = valor >= 0 ? 1 : -1;
    const valorAbs = Math.abs(valor);
    let parteInteira = Math.floor(valorAbs);
    const parteDecimal = valorAbs - parteInteira;
    if (parteDecimal >= 0.30) {
        parteInteira += 1;
    }
    const resultado = sinal * parteInteira;
    return resultado.toFixed(2);
}

const wss = new WebSocket.Server({ port: 8080 });

let enviadoHoje = false;
let ultimoTimestampEnviado = null;
let tipoAgregacao = 'media';
let dia = null;
let horaInicial = null;

wss.on('connection', async (ws) => {
    console.log('Cliente conectado ao WebSocket');

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            if (data.tipoAgregacao) {
                tipoAgregacao = data.tipoAgregacao;
                console.log(`Tipo de agregação alterado para: ${tipoAgregacao}`);
            }
            if (data.dia && data.horaInicial) {
                dia = data.dia;
                horaInicial = data.horaInicial;
                console.log(`Parâmetros recebidos: dia=${dia}, horaInicial=${horaInicial}`);
            }
        } catch (e) {
            console.error('Erro ao interpretar mensagem do cliente:', e);
        }
    });

    try {
        if (!dia || !horaInicial) {
            console.error('Parâmetros dia e horaInicial não foram enviados pelo cliente.');
            return;
        }

        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute(
            "SELECT * FROM grafico WHERE DATE(timestamp) = ? AND TIME(timestamp) >= ? ORDER BY timestamp ASC",
            [dia, horaInicial]
        );
        await conn.end();

        const agrupado = agruparPorIntervalo(rows, 5, tipoAgregacao);

        if (agrupado.length > 0) {
            ultimoTimestampEnviado = agrupado[agrupado.length - 1].fim;
            enviadoHoje = true;
            ws.send(JSON.stringify({ tipo: 'full', dados: agrupado }));
            console.log('Enviado histórico inicial para novo cliente:', agrupado.length);
        } else {
            console.warn('Nenhum dado encontrado para os parâmetros informados.');
            ws.send(JSON.stringify({ tipo: 'full', dados: [] }));
        }
    } catch (err) {
        console.error('Erro ao enviar dados iniciais:', err);
    }
});

// Função para agrupar por intervalos fixos
function agruparPorIntervalo(rows, intervaloMinutos = 5, tipoAgregacao = 'media') {
    if (!rows || rows.length === 0) return [];
    const inicioDia = new Date(rows[0].timestamp);
    inicioDia.setSeconds(0, 0);
    const fimDia = new Date(rows[rows.length - 1].timestamp);
    const agrupado = [];
    let intervaloInicio = new Date(inicioDia);

    while (intervaloInicio <= fimDia) {
        const intervaloFim = new Date(intervaloInicio.getTime() + intervaloMinutos * 60000);
        const bloco = rows.filter(r => {
            const ts = new Date(r.timestamp);
            return ts >= intervaloInicio && ts < intervaloFim;
        });

        let agregado;
        if (bloco.length > 0) {
            agregado = calcularAgregacao(bloco, tipoAgregacao, intervaloInicio, intervaloFim);
        } else {
            agregado = {
                inicio: intervaloInicio.toISOString(),
                fim: intervaloFim.toISOString(),
                alta: 0, queda: 0, neutro: 0,
                acumulado_alta: 0, acumulado_queda: 0, acumulado_neutro: 0,
                rastro_parcial: 0, rastro_acumulado: 0
            };
        }
        agrupado.push(agregado);
        intervaloInicio = intervaloFim;
    }
    return agrupado;
}

// Função para calcular agregação
function calcularAgregacao(bloco, tipo, intervaloInicio = null, intervaloFim = null) {
    const soma = {
        alta: 0, queda: 0, neutro: 0,
        acumulado_alta: 0, acumulado_queda: 0, acumulado_neutro: 0,
        rastro_parcial: 0, rastro_acumulado: 0
    };

    bloco.forEach(r => {
        soma.alta += Number(r.alta) || 0;
        soma.queda += Number(r.queda) || 0;
        soma.neutro += Number(r.neutro) || 0;
        soma.acumulado_alta += Number(r.acumulado_alta) || 0;
        soma.acumulado_queda += Number(r.acumulado_queda) || 0;
        soma.acumulado_neutro += Number(r.acumulado_neutro) || 0;
        soma.rastro_parcial += Number(r.rastro_parcial) || 0;
        soma.rastro_acumulado += Number(r.rastro_acumulado) || 0;
    });

    const n = bloco.length;
    if (tipo === 'media' && n > 0) {
        for (let key in soma) soma[key] = soma[key] / n;
    }

    // ✅ Aplicar arredondamento apenas se houver dados
    for (let key in soma) {
        soma[key] = arredondarCustom(soma[key]);
    }

    return {
        inicio: intervaloInicio ? intervaloInicio.toISOString() : bloco[0]?.timestamp,
        fim: intervaloFim ? intervaloFim.toISOString() : bloco[bloco.length - 1]?.timestamp,
        ...soma
    };
}

// Enviar dados a cada 5 minutos
setInterval(async () => {
    try {
        if (!dia || !horaInicial) return;

        const conn = await mysql.createConnection(dbConfig);
        const [rows] = await conn.execute(
            "SELECT * FROM grafico WHERE DATE(timestamp) = ? AND TIME(timestamp) >= ? ORDER BY timestamp ASC",
            [dia, horaInicial]
        );
        await conn.end();

        if (!enviadoHoje) {
            const agrupado = agruparPorIntervalo(rows, 5, tipoAgregacao);
            if (agrupado.length > 0) {
                ultimoTimestampEnviado = agrupado[agrupado.length - 1].fim;
                enviadoHoje = true;
                wss.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ tipo: 'full', dados: agrupado }));
                    }
                });
                console.log('Enviado todos os blocos do dia:', agrupado.length);
            } else {
                console.warn('Nenhum dado para enviar no histórico completo.');
            }
        } else {
            const novosRegistros = rows.filter(r => new Date(r.timestamp) > new Date(ultimoTimestampEnviado));
            if (novosRegistros.length > 0) {
                const intervaloInicio = new Date(ultimoTimestampEnviado);
                const intervaloFim = new Date(intervaloInicio.getTime() + 5 * 60000);
                const novoBloco = calcularAgregacao(novosRegistros, tipoAgregacao, intervaloInicio, intervaloFim);
                ultimoTimestampEnviado = novoBloco.fim;
                wss.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ tipo: 'incremento', dados: novoBloco }));
                    }
                });
                console.log('Enviado incremento:', novoBloco);
            }
        }
    } catch (err) {
        console.error('Erro ao enviar dados via WebSocket:', err);
    }
}, 5 * 60 * 1000);
