import request from '../utils/request'
import {TASK_LIST, GET_TASK_BY_ID, PAGE_SIZE} from '../constants'

const PATH = '/nacossync/v1/task/'
const initialState = {
    totalPage: 0,
    totalSize: 0,
    taskModel: {},
    taskModels: [],
    types: []
}

const add = data => request.post(`${PATH}add`, data)

const update = data => request.post(`${PATH}update`, data)

const list = params => dispatch => request.get(`${PATH}list`, {
    params: {
        ...params,
        pageSize: PAGE_SIZE
    }
}).then(data => dispatch({
    type: TASK_LIST,
    data
}))

const detail = params => dispatch => request.get(`${PATH}detail`, {params}).then(({taskModel}) => dispatch({
        type: GET_TASK_BY_ID,
        data: taskModel
    })
)

export default (state = initialState, action) => {
    switch (action.type) {
        case TASK_LIST:
            return {...state, ...action.data}
        case GET_TASK_BY_ID:
            return {...state, taskModel: action.data}
        default:
            return state
    }
}

export {list, add, update, detail}
