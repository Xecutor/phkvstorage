import * as React from "react";
import * as ReactDOM from "react-dom";

import {Label, Input, Button} from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

export interface VolumeInfo{
    volumePath:string
    volumeName:string
    mountPointPath:string
    volumeId:number
}

interface VolumesTabState{
    volumePath:string
    volumeName:string
    mountPointPath:string
}

interface VolumesTabProps{
    volumes:VolumeInfo[]
    reloadVolumes:()=>void
}

export class VolumesTab extends React.Component<VolumesTabProps, VolumesTabState>{
    constructor(props:VolumesTabProps) {
        super(props)
        this.state={
            volumePath:'.',
            volumeName:'volumeName',
            mountPointPath:'/'
        }
    }

    onVolumePathChanged(e:any, {value}:{value:string})
    {
        this.setState({volumePath:value});
    }
    onVolumePathChangedBound = this.onVolumePathChanged.bind(this)
    
    onVolumeNameChanged(e:any,{value}:{value:string})
    {
        this.setState({volumeName:value})
    }
    onVolumeNameChangedBound = this.onVolumeNameChanged.bind(this)

    onMountPointPathChanged(e:any,{value}:{value:string})
    {
        this.setState({mountPointPath:value})
    }
    onMountPointPathChangedBound = this.onMountPointPathChanged.bind(this)

    onCreateClick()
    {
        jsonrpcCall("create_and_mount_volume",{
            volumePath:this.state.volumePath,
            volumeName:this.state.volumeName,
            mountPointPath:this.state.mountPointPath
        }).then(()=>this.props.reloadVolumes())
    }
    onCreateClickBound = this.onCreateClick.bind(this)

    onMountClick()
    {
        jsonrpcCall("mount_volume",{
            volumePath:this.state.volumePath,
            volumeName:this.state.volumeName,
            mountPointPath:this.state.mountPointPath
        }).then(()=>this.props.reloadVolumes())
    }
    onMountClickBound = this.onMountClick.bind(this)

    render() {
        return <div>
            <Label>Volume path:</Label><Input value={this.state.volumePath} onChange={this.onVolumePathChangedBound}/>
            <Label>Volume name:</Label><Input value={this.state.volumeName} onChange={this.onVolumeNameChangedBound}/>
            <Label>Mount point path:</Label><Input value={this.state.mountPointPath} onChange={this.onMountPointPathChangedBound}/>
            <Button onClick={this.onCreateClickBound}>Create</Button><Button onClick={this.onMountClickBound}>Mount</Button>
        </div>
    }
}