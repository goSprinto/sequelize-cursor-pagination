const {
  Op,
  parseCursor,
  createCursor,
  normalizeOrder,
  getPaginationQuery,
  reverseOrder,
  isNil,
} = require('./utils');

/**
 * Adds a paginate method to a Sequelize model to support cursor-based pagination.
 *
 * This implementation supports selecting specific columns using `selectColumns`, which avoids
 * unnecessary `GROUP BY` clauses and improves performance in queries that do not require
 * aggregation. In cases where aggregate functions like `SUM`, `COUNT`, etc. are needed, use
 * `attributes` instead.
 *
 * @function paginate
 * @async
 * @param {Object} options - Pagination options
 * @param {Array} [options.order] - Ordering for the result set, e.g., [['createdAt', 'DESC']]
 * @param {Object} [options.where] - Filter conditions for the query
 * @param {string} [options.after] - Cursor string to fetch results after a specific node
 * @param {string} [options.before] - Cursor string to fetch results before a specific node
 * @param {number} [options.limit] - Maximum number of records to return
 * @param {Array<string>} [options.selectColumns] - Specific columns to select; avoids GROUP BY and improves performance for non-aggregated queries
 * @param {...Object} [queryArgs] - Additional Sequelize `findAll` query options
 *
 * @returns {Promise<Object>} An object containing:
 * - `totalCount` {number}: Total number of matching records (ignores pagination)
 * - `edges` {Array<Object>}: Array of nodes and their cursors
 * - `pageInfo` {Object}: Pagination metadata
 *    - `hasNextPage` {boolean}
 *    - `hasPreviousPage` {boolean}
 *    - `startCursor` {string|null}
 *    - `endCursor` {string|null}
 *
 * @example
 * await db.User.paginate({
 *   where: { isActive: true },
 *   limit: 10,
 *   order: [['createdAt', 'DESC']],
 *   selectColumns: ['id', 'email', 'createdAt'],
 *   after: 'cursorString',
 * });
 */
const withPagination =
  (options = {}) =>
  (Model) => {
    const {
      methodName = 'paginate',
      primaryKeyField = 'id',
      omitPrimaryKeyFromOrder = false,
      enforceNullOrder = true,
    } = options;

    const paginate = async ({
      order: orderOption,
      where,
      after,
      before,
      limit,
      selectColumns,
      ...queryArgs
    } = {}) => {
      let order = normalizeOrder(
        orderOption,
        primaryKeyField,
        omitPrimaryKeyFromOrder,
      );

      order = before ? reverseOrder(order, enforceNullOrder) : order;

      const cursor = after
        ? parseCursor(after)
        : before
        ? parseCursor(before)
        : null;

      const paginationQuery = cursor ? getPaginationQuery(order, cursor) : null;

      const paginationWhere = paginationQuery
        ? { [Op.and]: [paginationQuery, where] }
        : where;
      
      const paginationAttributes = !isNil(selectColumns) ? { attributes: selectColumns } : {};

      const paginationQueryOptions = {
        where: paginationWhere,
        limit,
        order,
        ...paginationAttributes,
        ...queryArgs,
      };

      const totalCountQueryOptions = {
        where,
        ...queryArgs,
      };

      const cursorCountQueryOptions = {
        where: paginationWhere,
        ...queryArgs,
      };

      const [instances, totalCount, cursorCount] = await Promise.all([
        Model.findAll(paginationQueryOptions),
        Model.count(totalCountQueryOptions),
        Model.count(cursorCountQueryOptions),
      ]);

      if (before) {
        instances.reverse();
      }

      const remaining = cursorCount - instances.length;

      const hasNextPage =
        (!before && remaining > 0) ||
        (Boolean(before) && totalCount - cursorCount > 0);

      const hasPreviousPage =
        (Boolean(before) && remaining > 0) ||
        (!before && totalCount - cursorCount > 0);

      const edges = instances.map((node) => ({
        node,
        cursor: createCursor(node, order),
      }));

      const pageInfo = {
        hasNextPage,
        hasPreviousPage,
        startCursor: edges.length > 0 ? edges[0].cursor : null,
        endCursor: edges.length > 0 ? edges[edges.length - 1].cursor : null,
      };

      return {
        totalCount,
        edges,
        pageInfo,
      };
    };

    Model[methodName] = paginate;

    return Model;
  };

module.exports = withPagination;
