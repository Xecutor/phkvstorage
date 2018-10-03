import * as React from "react";
import * as ReactDOM from "react-dom";

import {Label, Input, Segment, Button, Dropdown} from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

interface StoreTabState{
    keyPath:string
    type:string
    value:string
    result:string
}

interface StoreTabProps{
}

const valueTypes = [
    'uint8',
    'uin16',
    'uint32',
    'uint64',
    'float',
    'double',
    'string',
    'blob'
]

function makeTypeOptions(selected:string)
{
    return valueTypes.map(vt=>({key:vt, value:vt, text:vt, selected:vt==selected}))
}

export class StoreTab extends React.Component<StoreTabProps, StoreTabState>{
    constructor(props:StoreTabProps) {
        super(props)
        this.state={
            keyPath:'/key',
            type:'',
            value:'',
            result:''
        }
    }

    onKeyPathChanged(e:any, {value}:{value:string})
    {
        this.setState({keyPath:value, result:''});
    }
    onKeyPathChangedBound = this.onKeyPathChanged.bind(this)

    onResult(result:any)
    {
        console.log("result", result);
        this.setState({result:"Stored"})
    }
    onResultBound = this.onResult.bind(this)

    onFailure(reason:any)
    {
        console.log("failure", reason);
        this.setState({result:reason})
    }
    onFailureBound = this.onFailure.bind(this)

    onStoreClick()
    {
        console.log("store");
        jsonrpcCall("store", {key:this.state.keyPath, type:this.state.type, value:this.state.value}).then(this.onResultBound, this.onFailureBound)
    }
    onStoreClickBound = this.onStoreClick.bind(this)

    onTypeChanged(e:any, {value}:{value:string})
    {
        this.setState({type:value, result:''})
    }
    onTypeChangedBound = this.onTypeChanged.bind(this)

    onValueChanged(e:any, {value}:{value:string})
    {
        this.setState({value, result:''})
    }
    onValueChangedBound = this.onValueChanged.bind(this)

    render() {
        let result
        if(this.state.result.length)
        {
            result = <Segment key="result"><Label>Result</Label>{this.state.result}</Segment>
        }
        return <Segment.Group>
            {result}
            <Segment key="keypath"><Input label="Key path:" value={this.state.keyPath} onChange={this.onKeyPathChangedBound}/></Segment>
            <Segment key="type"><Label>Type:</Label><Dropdown label="Type" simple item options={makeTypeOptions(this.state.type)} onChange={this.onTypeChangedBound}/></Segment>
            <Segment key="value"><Input label="Value:" value={this.state.value} onChange={this.onValueChangedBound}/></Segment>
            <Segment key="storebutton"><Button onClick={this.onStoreClickBound}>Store</Button></Segment>
        </Segment.Group>
    }
}
