// ⚠️ No importes node-fetch. Node 18 en Netlify ya tiene fetch global.

exports.handler = async (event, context) => {
  try {
    const STORE   = process.env.SHOPIFY_STORE;       // ej: https://tutienda.myshopify.com
    const TOKEN   = process.env.SHOPIFY_API_KEY;     // shpat_...
    const VERSION = process.env.SHOPIFY_API_VERSION || '2024-07';

    // Validación para evitar "Invalid URL"
    const missing = [];
    if (!STORE)   missing.push('SHOPIFY_STORE');
    if (!TOKEN)   missing.push('SHOPIFY_API_KEY');
    if (!VERSION) missing.push('SHOPIFY_API_VERSION');

    try { new URL(STORE); } catch { missing.push('SHOPIFY_STORE (URL inválida; incluye https://)'); }
    if (missing.length) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Faltan variables', missing }) };
    }

    const today = new Date().toISOString().split('T')[0];
    const actualizados = [];

    // 1) Listar productos (nota: limita a 50; para más, implementa paginación con page_info)
    const res = await fetch(`${STORE}/admin/api/${VERSION}/products.json?limit=50`, {
      headers: {
        'X-Shopify-Access-Token': TOKEN,
        'Content-Type': 'application/json'
      }
    });
    if (!res.ok) {
      const text = await res.text();
      return { statusCode: res.status, body: JSON.stringify({ step: 'fetch_products', text }) };
    }
    const { products = [] } = await res.json();

    for (const product of products) {
      const productId = product.id;

      // 2) Leer metafields del producto
      const metaRes = await fetch(`${STORE}/admin/api/${VERSION}/products/${productId}/metafields.json`, {
        headers: {
          'X-Shopify-Access-Token': TOKEN,
          'Content-Type': 'application/json'
        }
      });
      if (!metaRes.ok) continue;
      const { metafields = [] } = await metaRes.json();

      const fechaMeta = metafields.find(m => m.namespace === 'custom' && m.key === 'fecha_disponibilidad');
      const stockMeta = metafields.find(m => m.namespace === 'custom' && m.key === 'stock_programado');
      if (!fechaMeta || !stockMeta) continue;

      const fechaDisponible = String(fechaMeta.value).slice(0, 10);
      const cantidadStock   = parseInt(stockMeta.value || '0', 10);

      if (fechaDisponible === today && cantidadStock > 0) {
        const variantId = product?.variants?.[0]?.id;
        if (!variantId) continue;

        // 3) Actualizar inventario de la primera variante
        const upd = await fetch(`${STORE}/admin/api/${VERSION}/variants/${variantId}.json`, {
          method: 'PUT',
          headers: {
            'X-Shopify-Access-Token': TOKEN,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            variant: {
              id: variantId,
              inventory_quantity: cantidadStock,
              inventory_management: 'shopify'
            }
          })
        });

        if (upd.ok) {
          actualizados.push({ productId, variantId, cantidadStock, fechaDisponible });
        } else {
          actualizados.push({ productId, variantId, error: await upd.text() });
        }
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Proceso completado ok', date: today, actualizados }, null, 2)
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err?.message || String(err) }) };
  }
};
