import * as React from "react";
import * as ReactDOM from "react-dom";

import {Button} from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

interface DataTabState{
}

interface DataTabProps{
}

export class DataTab extends React.Component<DataTabProps, DataTabState>{
    constructor(props:DataTabProps) {
        super(props)
        this.state={
        }
    }


    render() {
        return <div>
        data
        </div>
    }
}
