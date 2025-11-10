require('dotenv').config();
const mysql = require('mysql2/promise');

async function atualizarGrafico() {
    const conn = await mysql.createConnection({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME
    });

    // Buscar os dois últimos timestamps
    const [timestamps] = await conn.execute(`
        SELECT DISTINCT timestamp FROM ativos ORDER BY timestamp DESC LIMIT 2
    `);

    if (timestamps.length < 2) {
        console.log('Não há dados suficientes para calcular acumulados.');
        await conn.end();
        return;
    }

    const ultimoTimestamp = timestamps[0].timestamp;
    const penultimoTimestamp = timestamps[1].timestamp;

    // Contagem atual (gráfico principal) - apenas ativos de segurança
    const [contagemAtual] = await conn.execute(`
        SELECT
            SUM(CASE WHEN sinal = 'Alta' THEN 1 ELSE 0 END) AS alta,
            SUM(CASE WHEN sinal = 'Queda' THEN 1 ELSE 0 END) AS queda,
            SUM(CASE WHEN sinal = 'Neutro' THEN 1 ELSE 0 END) AS neutro
        FROM ativos
        WHERE timestamp = ? AND tipo_contagem = 'simple'
    `, [ultimoTimestamp]);

    const altaAtual = contagemAtual[0].alta || 0;
    const quedaAtual = contagemAtual[0].queda || 0;
    const neutroAtual = contagemAtual[0].neutro || 0;

    // Buscar ativos do último timestamp com código e limites
    const [ativosUltimo] = await conn.execute(`
        SELECT a.nome, a.variacao, r.codigo, r.limite_min_acumulado, r.limite_max_acumulado
        FROM ativos a
        JOIN regras_ativos r ON a.nome = r.nome
        WHERE a.timestamp = ? AND a.tipo_contagem = 'simple'
    `, [ultimoTimestamp]);

    // Buscar ativos do penúltimo timestamp com código
    const [ativosPenultimo] = await conn.execute(`
        SELECT a.nome, a.variacao, r.codigo
        FROM ativos a
        JOIN regras_ativos r ON a.nome = r.nome
        WHERE a.timestamp = ? AND a.tipo_contagem = 'simple'
    `, [penultimoTimestamp]);

    // Mapear penúltimo para acesso rápido por código
    const mapPenultimo = {};
    for (const a of ativosPenultimo) {
        mapPenultimo[a.codigo] = a.variacao;
    }

    // Calcular acumulados com base na diferença e limites
    let acumuladoAlta = 0, acumuladoQueda = 0, acumuladoNeutro = 0;

    for (const a of ativosUltimo) {
        const anterior = mapPenultimo[a.codigo];
        if (anterior !== undefined) {
            const diff = a.variacao - anterior;
            const limiteMin = a.limite_min_acumulado ?? -0.1;
            const limiteMax = a.limite_max_acumulado ?? 0.1;

            if (diff >= limiteMin && diff <= limiteMax) {
                acumuladoNeutro++;
            } else if (diff > limiteMax) {
                acumuladoQueda++;
            } else {
                acumuladoAlta++;
            }
        }
    }

    // Calcular rastro parcial e acumulado
    const [ultimoHoje] = await conn.execute(`
        SELECT * FROM grafico
        WHERE DATE(timestamp) = CURDATE()
        ORDER BY timestamp DESC
        LIMIT 1
    `);

    const rastroParcial = acumuladoAlta - acumuladoQueda;
    const rastroAnterior = ultimoHoje.length > 0 ? ultimoHoje[0].rastro_acumulado : 0;
    const rastroAcumulado = rastroAnterior + rastroParcial;

    // Inserir no banco
    await conn.execute(`
        INSERT INTO grafico (
            alta, queda, neutro,
            acumulado_alta, acumulado_queda, acumulado_neutro,
            rastro_parcial, rastro_acumulado,
            timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
        altaAtual, quedaAtual, neutroAtual,
        acumuladoAlta, acumuladoQueda, acumuladoNeutro,
        rastroParcial, rastroAcumulado,
        new Date()
    ]);

    await conn.end();
    console.log('Gráfico atualizado com sucesso!');
}

module.exports = { atualizarGrafico };